"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useTrader, Exchange } from "../../context";
import {
  allocatorApi,
  type ApiCapitalEvent,
  type ApiPnl,
} from "../../api";
import { ExchangeLinkWizard } from "../../components/ExchangeLinkWizard";
import { useConfirm } from "../../../components/ConfirmDialog";

// ─── Shared form styling — also used by the extracted ExchangeLinkWizard
//     component (which keeps its own duplicate, see file header of
//     components/ExchangeLinkWizard.tsx for the dedup note). They live HERE
//     because the Capital Events add form and the Baseline Anchor edit
//     modal further down this page rely on them too. ──────────────────────────

const fieldLabelStyle: React.CSSProperties = {
  fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em",
  fontWeight: 700, textTransform: "uppercase", marginBottom: 6,
};

const inputStyle: React.CSSProperties = {
  width: "100%", background: "var(--bg3)", border: "1px solid var(--line)",
  borderRadius: 3, padding: "9px 12px", color: "var(--t0)",
  fontSize: 10, outline: "none",
  fontFamily: "var(--font-space-mono), Space Mono, monospace",
};

const primaryBtnStyle: React.CSSProperties = {
  padding: "10px 20px", background: "var(--green)", color: "var(--bg0)",
  border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700,
  letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer",
};

const disabledBtnStyle: React.CSSProperties = {
  ...primaryBtnStyle,
  background: "var(--bg2)", color: "var(--t2)", cursor: "not-allowed",
};

// ─── Page ────────────────────────────────────────────────────────────────────

// ─── Status pill (shared between cards + summary table) ─────────────────────

type PillTheme = { label: string; color: string; bg: string; border: string };

function statusTheme(status: Exchange["status"]): PillTheme {
  switch (status) {
    case "active":
      return { label: "CONNECTED", color: "var(--green)", bg: "var(--green-dim)", border: "var(--green-mid)" };
    case "pending_validation":
      return { label: "VALIDATING", color: "var(--amber)", bg: "var(--amber-dim)", border: "var(--amber)" };
    case "invalid":
      return { label: "INVALID", color: "var(--red)", bg: "var(--red-dim)", border: "var(--red)" };
    case "errored":
      return { label: "ERROR", color: "var(--amber)", bg: "var(--amber-dim)", border: "var(--amber)" };
    case "revoked":
      return { label: "REVOKED", color: "var(--t3)", bg: "var(--bg2)", border: "var(--line)" };
  }
}

function StatusPill({ status, size = "md" }: { status: Exchange["status"]; size?: "sm" | "md" }) {
  const t = statusTheme(status);
  return (
    <span style={{
      fontSize: size === "sm" ? 8 : 9,
      fontWeight: 700, letterSpacing: "0.12em",
      padding: size === "sm" ? "2px 6px" : "3px 8px",
      borderRadius: 3,
      background: t.bg, color: t.color, border: `1px solid ${t.border}`,
      textTransform: "uppercase",
    }}>
      {t.label}
    </span>
  );
}

// ConfirmRemoveModal removed in phase-1c/final-polish-bundle —
// migrated to the shared ConfirmDialog primitive (PR #45) via
// useConfirm() in the row's onClick handler.

// ─── Capital Events section ─────────────────────────────────────────────────
//
// Operator-recorded out-of-band capital movements. Subtracted from PnL math
// in /pnl so trading returns aren't conflated with principal moves. Today
// these are entered by hand; an exchange income API auto-importer is
// deferred (see docs/open_work_list.md "Capital change tracking").

const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";

// Max events rendered per panel by default. Walk of ~36 rows per exchange is
// noisy; showing recent 10 is enough for at-a-glance diagnosis. Full list
// available via the "Show all N" toggle at the bottom.
const CAPITAL_EVENTS_DEFAULT_LIMIT = 10;

function fmtCapitalDate(iso: string | null): string {
  if (!iso) return "—";
  // YYYY-MM-DD HH:MM (UTC), trim seconds
  const d = new Date(iso);
  const pad = (n: number) => n.toString().padStart(2, "0");
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} `
    + `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
  );
}

function fmtAmount(usd: number, kind: "deposit" | "withdrawal"): string {
  const sign = kind === "deposit" ? "+" : "−";
  return `${sign}$${usd.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

/**
 * Abbreviate a capital-event notes string for in-row display. Full text
 * remains on a hover tooltip. Rules:
 *  - Notes starting with "<number> <ASSET>" → non-stablecoin deposit, show
 *    just the amount rounded to 5 decimals + asset (e.g., "14.53774 SOL").
 *  - Otherwise → look for chain=X or network=X and show the value (e.g.,
 *    "Polygon POS" or "MATIC").
 *  - Fallback: first 30 characters of the notes.
 *  - Empty/null → em-dash.
 */
function abbreviateNotes(notes: string | null): string {
  if (!notes) return "—";
  // Non-stablecoin pattern: first segment is "<amount> <ASSET>".
  const firstSeg = notes.split(";")[0].trim();
  const tokenMatch = firstSeg.match(/^([\d.]+)\s+([A-Z]{2,10})$/);
  if (tokenMatch) {
    const amt = parseFloat(tokenMatch[1]);
    const asset = tokenMatch[2];
    if (!Number.isNaN(amt)) {
      return `${amt.toFixed(5)} ${asset}`;
    }
  }
  // Stablecoin pattern: find chain=... or network=...
  const chainMatch = notes.match(/(?:chain|network)=([^;]+)/);
  if (chainMatch) return chainMatch[1].trim();
  // Fallback.
  return notes.length > 30 ? notes.slice(0, 30) + "…" : notes;
}

// ─── Per-exchange capital-events panel ─────────────────────────────────────
//
// Renders one exchange connection: the allocations hanging off it (with
// principal summaries + Edit-anchor link) followed by a table of the
// events physically tied to that connection. Each row has a Credit-To
// dropdown scoped to THAT connection's allocations — you can't credit a
// BloFin deposit to a Binance allocation, and vice versa.

function ExchangeCapitalGroup({
  group,
  exchangeBalance,
  exchangeAnchorAt,
  exchangeBaselineUsd,
  onEditAnchor,
  onUpdateEvent,
  onDeleteEvent,
  onRecordNew,
  onReset,
}: {
  group: {
    connection_id: string | null;
    exchange_name: string | null;
    allocations: { id: string; label: string }[];
    events: ApiCapitalEvent[];
  };
  /** Current total equity on this exchange from Exchange.balance (null if
   * not fetched yet or for the orphan "no connection" bucket). */
  exchangeBalance: number | null;
  /** Operator-set anchor on the connection (null = no override). */
  exchangeAnchorAt: string | null;
  /** Operator-set baseline USD (null = no override, treated as 0 for math). */
  exchangeBaselineUsd: number | null;
  /** Opens the exchange-level anchor modal for this connection. */
  onEditAnchor: (connectionId: string) => void;
  onUpdateEvent: (ev: ApiCapitalEvent) => void;
  onDeleteEvent: (ev: ApiCapitalEvent) => void;
  // Optional — only present on real exchange panels (not the orphan "no
  // exchange link" bucket, where these actions don't apply).
  onRecordNew?: () => void;
  onReset?: () => void;
}) {
  // Exchange-level principal math (matches /pnl backend formula):
  //   principal   = baseline + SUM(deposits − withdrawals since anchor)
  //   balance     = current equity on the exchange
  //   trading_Δ   = balance − principal
  // Events passed in here are already filtered by the parent to those on
  // or after the anchor, so this SUM is the net-since-anchor.
  const netSinceAnchor = group.events.reduce((sum, ev) => {
    const signed = ev.kind === "deposit" ? ev.amount_usd : -ev.amount_usd;
    return sum + signed;
  }, 0);
  const baseline = exchangeBaselineUsd ?? 0;
  const principal = baseline + netSinceAnchor;
  const tradingDelta = exchangeBalance !== null ? exchangeBalance - principal : null;

  // Expansion state: default COLLAPSED so the at-a-glance principal bar is
  // what you see first; click the header chevron to reveal the events table.
  const [expanded, setExpanded] = useState<boolean>(false);
  // Within expanded state, optionally show all events (default truncated
  // to CAPITAL_EVENTS_DEFAULT_LIMIT most recent).
  const [showAllEvents, setShowAllEvents] = useState<boolean>(false);
  const totalEvents = group.events.length;
  const truncated = !showAllEvents && totalEvents > CAPITAL_EVENTS_DEFAULT_LIMIT;
  const visibleEvents = truncated
    ? group.events.slice(0, CAPITAL_EVENTS_DEFAULT_LIMIT)
    : group.events;

  const header = group.exchange_name
    ? group.exchange_name.toUpperCase()
    : "UNASSIGNED (no exchange link)";

  return (
    <div style={{
      background: "var(--bg1)", border: "1px solid var(--line)",
      borderRadius: 6, overflow: "hidden",
    }}>
      {/* Exchange header with inline RESET + RECORD buttons */}
      <div style={{
        padding: "10px 14px",
        borderBottom: "1px solid var(--line)",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        gap: 12,
      }}>
        <div
          onClick={() => setExpanded(v => !v)}
          style={{
            display: "flex", alignItems: "center", gap: 12, minWidth: 0,
            cursor: "pointer", userSelect: "none", flex: 1,
          }}
          title={expanded ? "Collapse events table" : "Expand to show events"}
        >
          <span style={{
            color: "var(--t2)", fontSize: 10, width: 12,
            display: "inline-block", textAlign: "center",
          }}>
            {expanded ? "▾" : "▸"}
          </span>
          <span style={{
            fontSize: 11, fontWeight: 700, color: "var(--t0)",
            letterSpacing: "0.05em",
          }}>{header}</span>
          <span style={{
            fontSize: 9, color: "var(--t3)",
            letterSpacing: "0.12em", textTransform: "uppercase",
          }}>
            {group.allocations.length} alloc · {group.events.length} event{group.events.length === 1 ? "" : "s"}
          </span>
        </div>
        {(onReset || onRecordNew || group.connection_id) && (
          <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
            {group.connection_id && (
              <button
                onClick={() => onEditAnchor(group.connection_id!)}
                title="Set this exchange's principal anchor date + baseline"
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 3, color: "var(--t1)",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", padding: "5px 12px",
                  cursor: "pointer", fontFamily: FONT_MONO,
                }}
                onMouseEnter={e => (e.currentTarget.style.color = "var(--t0)")}
                onMouseLeave={e => (e.currentTarget.style.color = "var(--t1)")}
              >EDIT ANCHOR</button>
            )}
            {onReset && (
              <button
                onClick={onReset}
                disabled={group.allocations.length === 0 && group.events.length === 0}
                title={`Delete manual entries + overrides on ${group.exchange_name ?? "this exchange"} and re-sync from the exchange.`}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 3, color: "var(--amber)",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", padding: "5px 12px",
                  cursor: "pointer", fontFamily: FONT_MONO,
                }}
              >RESET DEFAULT</button>
            )}
            {onRecordNew && (
              <button
                onClick={onRecordNew}
                disabled={group.allocations.length === 0}
                title={group.allocations.length === 0
                  ? "No allocations on this exchange yet"
                  : `Add an entry on ${group.exchange_name ?? "this exchange"}`}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 3,
                  color: group.allocations.length === 0 ? "var(--t3)" : "var(--green)",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", padding: "5px 12px",
                  cursor: group.allocations.length === 0 ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >+ ADD ENTRY</button>
            )}
          </div>
        )}
      </div>

      {/* Exchange-level principal summary. Kept to a single line — the
          per-event PRINCIPAL AFTER column already exposes the baseline +
          net-since-anchor breakdown row-by-row, so this bar just shows
          the headline numbers. */}
      {group.connection_id && (
        <div style={{
          padding: "10px 14px",
          borderBottom: "1px solid var(--line)",
          background: "var(--bg2)",
          display: "flex", alignItems: "center", gap: 18,
          fontSize: 10, fontFamily: FONT_MONO,
          whiteSpace: "nowrap", overflow: "hidden",
        }}>
          <span style={{ display: "flex", gap: 6, alignItems: "baseline" }}>
            <span style={{ color: "var(--t3)" }}>Bal:</span>
            <span style={{ color: "var(--t0)", fontWeight: 700 }}>
              {exchangeBalance !== null
                ? `$${exchangeBalance.toLocaleString("en-US", { maximumFractionDigits: 2 })}`
                : "—"}
            </span>
          </span>
          <span style={{ display: "flex", gap: 6, alignItems: "baseline" }}>
            <span style={{ color: "var(--t3)" }}>Prin:</span>
            <span style={{ color: "var(--t0)", fontWeight: 700 }}>
              ${principal.toLocaleString("en-US", { maximumFractionDigits: 2 })}
            </span>
          </span>
          {tradingDelta !== null && (() => {
            // Near-zero gate: anything within ±$1 renders amber, not
            // red/green. Avoids visual noise on immaterial drift.
            const deltaColor = Math.abs(tradingDelta) < 1
              ? "var(--amber)"
              : tradingDelta >= 0 ? "var(--green)" : "var(--red)";
            const pctValue = principal > 0
              ? tradingDelta / principal * 100
              : null;
            return (
              <span style={{ display: "flex", gap: 6, alignItems: "baseline" }}>
                <span style={{ color: "var(--t3)" }}>PnL:</span>
                <span style={{ color: deltaColor, fontWeight: 700 }}>
                  {tradingDelta >= 0 ? "+" : "−"}$
                  {Math.abs(tradingDelta).toLocaleString("en-US", { maximumFractionDigits: 2 })}
                  {pctValue !== null && (
                    <span style={{ fontWeight: 400, marginLeft: 4 }}>
                      ({pctValue >= 0 ? "+" : "−"}
                      {Math.abs(pctValue).toFixed(2)}%)
                    </span>
                  )}
                </span>
              </span>
            );
          })()}
          <span style={{
            color: "var(--t3)", marginLeft: "auto",
            fontStyle: exchangeAnchorAt ? "normal" : "italic",
          }}>
            {exchangeAnchorAt
              ? `since ${exchangeAnchorAt.slice(0, 10)}`
              : "no anchor set"}
          </span>
        </div>
      )}

      {/* Per-allocation metrics intentionally NOT rendered here. This
          panel is exchange-scoped bookkeeping; per-allocation performance
          (capital, cumulative return, P&L) belongs on the Trader card.
          Mixing the two previously invited confusing apples-to-oranges
          comparisons (allocation.capital_usd vs exchange.principal are
          independently correct numbers that aren't expected to match). */}

      {/* Events table — hidden by default (panel collapsed); click the
          header chevron to expand. Within expanded state, events are
          truncated to CAPITAL_EVENTS_DEFAULT_LIMIT with a "Show all"
          toggle at the bottom. */}
      {expanded && (group.events.length === 0 ? (
        <div style={{ padding: "14px", fontSize: 10, color: "var(--t3)" }}>
          No capital events recorded for this exchange yet.
        </div>
      ) : (
        <>
        <table style={{
          width: "100%", borderCollapse: "collapse", fontSize: 10,
          fontFamily: FONT_MONO, tableLayout: "fixed",
        }}>
          <colgroup>
            <col style={{ width: "18%" }} />  {/* date */}
            <col style={{ width: "9%" }} />   {/* kind */}
            <col style={{ width: "13%" }} />  {/* amount */}
            <col style={{ width: "13%" }} />  {/* principal (running total after this event) */}
            <col />                             {/* notes */}
            <col style={{ width: "150px" }} />{/* actions */}
          </colgroup>
          <thead>
            <tr style={{ borderBottom: "1px solid var(--line)" }}>
              {["DATE (UTC)", "KIND", "AMOUNT", "PRINCIPAL AFTER", "NOTES", ""].map(h => (
                <th key={h} style={{
                  padding: "7px 14px", textAlign: "left",
                  fontSize: 9, color: "var(--t3)", fontWeight: 700,
                  letterSpacing: "0.12em", textTransform: "uppercase",
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(() => {
              // Running principal walk uses the FULL event list (not the
              // visible-limit slice) so 'PRINCIPAL AFTER' is accurate even
              // when we're only rendering the most-recent N rows.
              const byOldest = [...group.events].sort(
                (a, b) => a.event_at.localeCompare(b.event_at),
              );
              const principalAfter: Record<string, number> = {};
              let running = baseline;
              for (const ev of byOldest) {
                running += ev.kind === "deposit" ? ev.amount_usd : -ev.amount_usd;
                principalAfter[ev.event_id] = running;
              }
              return visibleEvents.map((ev, idx) => {
              const amountColor = ev.kind === "deposit" ? "var(--green)" : "var(--amber)";
              let sourceBg = "var(--bg2)", sourceColor = "var(--t2)", sourceLabel = "MANUAL";
              if (ev.source === "auto") {
                sourceBg = "var(--green-dim)"; sourceColor = "var(--green)"; sourceLabel = "AUTO";
              } else if (ev.source === "auto-anomaly") {
                sourceBg = "var(--amber-dim)"; sourceColor = "var(--amber)"; sourceLabel = "ANOMALY";
              }
              if (ev.is_manually_overridden && ev.source !== "manual") {
                sourceLabel += "*";
              }
              return (
                <tr key={ev.event_id} style={{
                  borderBottom: idx < visibleEvents.length - 1 ? "1px solid var(--line)" : "none",
                }}>
                  <td style={{ padding: "10px 14px", color: "var(--t1)" }}>
                    {fmtCapitalDate(ev.event_at)}
                    <span
                      title={
                        ev.source === "manual"
                          ? "Operator-entered"
                          : ev.source === "auto-anomaly"
                            ? "Auto-detected via mid-session equity-jump anomaly"
                            : "Auto-detected from exchange income API"
                          + (ev.is_manually_overridden ? " — edited by operator (won't re-sync)" : "")
                      }
                      style={{
                        marginLeft: 8,
                        display: "inline-block",
                        fontSize: 8, fontWeight: 700, letterSpacing: "0.08em",
                        padding: "1px 5px", borderRadius: 2,
                        background: sourceBg, color: sourceColor,
                        cursor: "help", verticalAlign: "middle",
                      }}
                    >
                      {sourceLabel}
                    </span>
                  </td>
                  <td style={{
                    padding: "10px 14px", color: "var(--t2)",
                    textTransform: "uppercase",
                  }}>{ev.kind === "withdrawal" ? "withdraw" : ev.kind}</td>
                  <td style={{
                    padding: "10px 14px", color: amountColor, fontWeight: 700,
                    whiteSpace: "nowrap",
                  }}>{fmtAmount(ev.amount_usd, ev.kind)}</td>
                  <td style={{
                    padding: "10px 14px", color: "var(--t1)",
                    whiteSpace: "nowrap", fontWeight: 700,
                  }}>
                    ${(principalAfter[ev.event_id] ?? 0)
                      .toLocaleString("en-US", { maximumFractionDigits: 2 })}
                  </td>
                  <td
                    title={ev.notes ?? ""}
                    style={{
                      padding: "10px 14px", color: "var(--t2)",
                      overflow: "hidden", textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                      cursor: ev.notes ? "help" : "default",
                    }}
                  >{abbreviateNotes(ev.notes)}</td>
                  <td style={{
                    padding: "10px 14px", textAlign: "right",
                    whiteSpace: "nowrap",
                  }}>
                    <button
                      onClick={() => onUpdateEvent(ev)}
                      style={{
                        background: "transparent", border: "none",
                        color: "var(--t1)", fontSize: 9, fontWeight: 700,
                        letterSpacing: "0.12em", textTransform: "uppercase",
                        marginRight: 12, cursor: "pointer",
                        fontFamily: FONT_MONO,
                      }}
                      onMouseEnter={e => (e.currentTarget.style.color = "var(--green)")}
                      onMouseLeave={e => (e.currentTarget.style.color = "var(--t1)")}
                    >Update</button>
                    <button
                      onClick={() => onDeleteEvent(ev)}
                      style={{
                        background: "transparent", border: "none",
                        color: "var(--t3)", fontSize: 9, fontWeight: 700,
                        letterSpacing: "0.12em", textTransform: "uppercase",
                        cursor: "pointer", fontFamily: FONT_MONO,
                      }}
                      onMouseEnter={e => (e.currentTarget.style.color = "var(--red)")}
                      onMouseLeave={e => (e.currentTarget.style.color = "var(--t3)")}
                    >Delete</button>
                  </td>
                </tr>
              );
              });
            })()}
          </tbody>
        </table>
        {totalEvents > CAPITAL_EVENTS_DEFAULT_LIMIT && (
          <div style={{
            padding: "8px 14px", borderTop: "1px solid var(--line)",
            display: "flex", justifyContent: "center",
          }}>
            <button
              onClick={() => setShowAllEvents(v => !v)}
              style={{
                background: "transparent", border: "none",
                color: "var(--t2)", fontSize: 9, fontWeight: 700,
                letterSpacing: "0.12em", textTransform: "uppercase",
                cursor: "pointer", fontFamily: FONT_MONO,
              }}
              onMouseEnter={e => (e.currentTarget.style.color = "var(--green)")}
              onMouseLeave={e => (e.currentTarget.style.color = "var(--t2)")}
            >
              {showAllEvents
                ? `Show only ${CAPITAL_EVENTS_DEFAULT_LIMIT} most recent`
                : `Show all ${totalEvents} events`}
            </button>
          </div>
        )}
        </>
      ))}
    </div>
  );
}


function CapitalEventModal({
  initial,
  allocations,
  submitting,
  onCancel,
  onSubmit,
}: {
  initial: ApiCapitalEvent | null; // null = create, set = edit
  allocations: { id: string; label: string }[];
  submitting: boolean;
  onCancel: () => void;
  onSubmit: (data: {
    allocation_id: string;
    amount_usd: number;
    kind: "deposit" | "withdrawal";
    event_at?: string;
    notes?: string;
  }) => void;
}) {
  // Default event_at to "now" formatted as YYYY-MM-DDTHH:MM (datetime-local).
  const nowLocal = (() => {
    const d = new Date();
    const pad = (n: number) => n.toString().padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
      + `T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  })();

  const [allocationId, setAllocationId] = useState(
    initial?.allocation_id ?? (allocations[0]?.id ?? ""),
  );
  const [kind, setKind] = useState<"deposit" | "withdrawal">(
    initial?.kind ?? "deposit",
  );
  const [amount, setAmount] = useState<string>(
    initial ? initial.amount_usd.toString() : "",
  );
  // The HTML datetime-local input wants local-time format. Initial values come
  // in as UTC ISO; convert by trimming the timezone suffix.
  const [eventAt, setEventAt] = useState<string>(
    initial
      ? initial.event_at.slice(0, 16)
      : nowLocal,
  );
  const [notes, setNotes] = useState<string>(initial?.notes ?? "");
  const [errorText, setErrorText] = useState<string | null>(null);

  const valid = allocationId
    && parseFloat(amount) > 0
    && (kind === "deposit" || kind === "withdrawal");

  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={() => { if (!submitting) onCancel(); }}
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background: "var(--bg2)", border: "1px solid var(--line2)",
          borderRadius: 6, padding: "20px 24px",
          width: 480, maxWidth: "92vw",
          fontFamily: FONT_MONO,
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
        }}>
          {initial ? "Edit capital event" : "Record capital event"}
        </div>

        <div style={{ marginBottom: 10 }}>
          <div style={fieldLabelStyle}>Allocation</div>
          <select
            value={allocationId}
            onChange={e => setAllocationId(e.target.value)}
            disabled={!!initial}  // immutable on edit (would orphan the row)
            style={{ ...inputStyle, appearance: "none", paddingRight: 28, cursor: initial ? "not-allowed" : "pointer" }}
          >
            {allocations.length === 0 && <option value="">— No allocations —</option>}
            {allocations.map(a => (
              <option key={a.id} value={a.id}>{a.label}</option>
            ))}
          </select>
        </div>

        <div style={{ display: "flex", gap: 10, marginBottom: 10 }}>
          <div style={{ flex: 1 }}>
            <div style={fieldLabelStyle}>Kind</div>
            <select
              value={kind}
              onChange={e => setKind(e.target.value as "deposit" | "withdrawal")}
              style={{ ...inputStyle, appearance: "none", paddingRight: 28, cursor: "pointer" }}
            >
              <option value="deposit">Deposit</option>
              <option value="withdrawal">Withdrawal</option>
            </select>
          </div>
          <div style={{ flex: 1 }}>
            <div style={fieldLabelStyle}>Amount (USD)</div>
            <input
              type="number"
              min="0"
              step="0.01"
              placeholder="0.00"
              value={amount}
              onChange={e => setAmount(e.target.value)}
              style={inputStyle}
              onFocus={e => (e.target.style.borderColor = "var(--green)")}
              onBlur={e => (e.target.style.borderColor = "var(--line)")}
            />
          </div>
        </div>

        <div style={{ marginBottom: 10 }}>
          <div style={fieldLabelStyle}>Event time (local)</div>
          <input
            type="datetime-local"
            value={eventAt}
            onChange={e => setEventAt(e.target.value)}
            style={inputStyle}
            onFocus={e => (e.target.style.borderColor = "var(--green)")}
            onBlur={e => (e.target.style.borderColor = "var(--line)")}
          />
        </div>

        <div style={{ marginBottom: 14 }}>
          <div style={fieldLabelStyle}>Notes (optional)</div>
          <input
            type="text"
            placeholder="e.g. Binance → BloFin transfer"
            value={notes}
            onChange={e => setNotes(e.target.value)}
            style={inputStyle}
            onFocus={e => (e.target.style.borderColor = "var(--green)")}
            onBlur={e => (e.target.style.borderColor = "var(--line)")}
          />
        </div>

        {errorText && (
          <div style={{ fontSize: 10, color: "var(--red)", marginBottom: 10 }}>
            {errorText}
          </div>
        )}

        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button
            type="button"
            disabled={submitting}
            onClick={onCancel}
            style={{
              background: "transparent", border: "1px solid var(--line2)",
              borderRadius: 4, padding: "8px 16px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase", color: "var(--t2)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: FONT_MONO,
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={!valid || submitting}
            onClick={() => {
              try {
                // datetime-local has no timezone — interpret as local, convert to UTC ISO.
                const eventAtIso = eventAt
                  ? new Date(eventAt).toISOString()
                  : undefined;
                onSubmit({
                  allocation_id: allocationId,
                  amount_usd: parseFloat(amount),
                  kind,
                  event_at: eventAtIso,
                  notes: notes.trim() || undefined,
                });
              } catch (e) {
                setErrorText(e instanceof Error ? e.message : String(e));
              }
            }}
            style={(!valid || submitting) ? disabledBtnStyle : primaryBtnStyle}
          >
            {submitting ? "Saving…" : (initial ? "Save" : "Record")}
          </button>
        </div>
      </div>
    </div>
  );
}

function CapitalEventsSection() {
  const { instances, exchanges, refresh: traderRefresh } = useTrader();
  const [events, setEvents] = useState<ApiCapitalEvent[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editingEvent, setEditingEvent] = useState<ApiCapitalEvent | null>(null);
  // When recording a new event, track which connection initiated so the
  // modal can scope the allocation dropdown + default-set connection_id.
  // null = modal closed; string = connection_id of the exchange panel
  // whose "+ RECORD" button was clicked.
  const [creatingForConnection, setCreatingForConnection] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<ApiCapitalEvent | null>(null);
  // confirmReset holds the connection_id being reset; null = modal closed.
  const [confirmReset, setConfirmReset] = useState<string | null>(null);
  const [editingAnchor, setEditingAnchor] = useState<string | null>(null);
  // Per-allocation /pnl response cache (allocation_id → full ApiPnl).
  const [pnlByAlloc, setPnlByAlloc] = useState<Record<string, ApiPnl>>({});

  const refresh = useCallback(async () => {
    try {
      const r = await allocatorApi.getCapitalEvents();
      setEvents(r.events);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const refreshPnl = useCallback(async (allocIds: string[]) => {
    const out: Record<string, ApiPnl> = {};
    await Promise.all(allocIds.map(async id => {
      try {
        out[id] = await allocatorApi.getPnl(id);
      } catch {
        // non-fatal; alloc simply won't render a principal summary
      }
    }));
    setPnlByAlloc(out);
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  // Build allocation dropdown options from useTrader.instances. Use
  // strategyName + exchangeName as the user-readable label; id is the
  // allocation_id the backend expects.
  const allocOptions = instances
    .filter(i => i.id && !i.id.startsWith("temp-"))
    .map(i => ({
      id: i.id,
      label: `${i.strategyName}${i.exchangeName ? ` · ${i.exchangeName}` : ""}`,
    }));

  const allocLabelById: Record<string, string> = {};
  for (const opt of allocOptions) allocLabelById[opt.id] = opt.label;

  // Fetch principal summary for each allocation after the picker materializes.
  // Refreshes on events-list change so edits propagate automatically.
  useEffect(() => {
    if (allocOptions.length > 0) refreshPnl(allocOptions.map(a => a.id));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instances, events]);

  async function handleCreate(data: Parameters<typeof allocatorApi.createCapitalEvent>[0]) {
    setSubmitting(true);
    try {
      await allocatorApi.createCapitalEvent(data);
      setCreatingForConnection(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleEdit(eventId: string, data: Parameters<typeof allocatorApi.updateCapitalEvent>[1]) {
    setSubmitting(true);
    try {
      await allocatorApi.updateCapitalEvent(eventId, data);
      setEditingEvent(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete(eventId: string) {
    setSubmitting(true);
    try {
      await allocatorApi.deleteCapitalEvent(eventId);
      setConfirmDelete(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleReset(connectionId: string) {
    setSubmitting(true);
    try {
      await allocatorApi.resetCapitalEventsToDefaults(connectionId);
      setConfirmReset(null);
      await refresh();
      // Silent success — the list refresh is the visual confirmation.
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleAnchorSave(
    connectionId: string,
    data: { anchor_at?: string; reset?: boolean },
  ) {
    setSubmitting(true);
    try {
      await allocatorApi.updateConnection(connectionId, data.reset ? {
        clear_principal_anchor: true,
        clear_principal_baseline: true,
      } : {
        // Only send the anchor; backend auto-derives baseline from
        // exchange_snapshots at the anchor moment.
        principal_anchor_at: data.anchor_at,
      });
      setEditingAnchor(null);
      await refresh();
      await refreshPnl(allocOptions.map(a => a.id));
      // Re-fetch exchanges so the Exchange.principalAnchorAt in useTrader
      // updates across all consumers (Trader card, event filter, etc.).
      await traderRefresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div style={{ marginTop: 20 }}>
      <div style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
      }}>
        CAPITAL EVENTS
      </div>

      {error && (
        <div style={{
          background: "var(--red-dim)", border: "1px solid var(--red)",
          borderRadius: 4, padding: "8px 12px", marginBottom: 8,
          fontSize: 10, color: "var(--red)",
        }}>
          {error}
        </div>
      )}

      {/* Exchange-first grouping. Each exchange connection gets its own
          panel with: the allocations hanging off it (principal summary +
          Edit anchor), then the events physically tied to that connection
          (CREDIT TO dropdown lets operator reassign to any allocation on
          the same connection). Events without a connection_id (legacy
          manual entries) fall into a "No exchange" bucket at the end. */}
      {events === null ? (
        <div style={{
          background: "var(--bg1)", border: "1px solid var(--line)",
          borderRadius: 6, padding: "16px", fontSize: 10, color: "var(--t2)",
        }}>
          Loading…
        </div>
      ) : (
        (() => {
          // Build connection-first grouping.
          type ConnectionGroup = {
            connection_id: string | null;
            exchange_name: string | null;  // null if the group has no events linked to a connection
            allocations: typeof allocOptions;
            events: ApiCapitalEvent[];
          };
          const groups: ConnectionGroup[] = [];
          const byConn: Record<string, ConnectionGroup> = {};
          // Seed from exchanges so we always show a panel per linked exchange,
          // even with zero events. Maps Exchange.id → connection_id (same in
          // our system — Exchange.id is the connection UUID).
          for (const ex of exchanges) {
            const key = ex.id;
            const group: ConnectionGroup = {
              connection_id: key,
              exchange_name: ex.name || ex.exchange,
              allocations: instances
                .filter(i => i.id && !i.id.startsWith("temp-") && i.connectionId === key)
                .map(i => ({
                  id: i.id,
                  label: `${i.strategyName}${i.exchangeName ? ` · ${i.exchangeName}` : ""}`,
                })),
              events: [],
            };
            byConn[key] = group;
            groups.push(group);
          }
          const orphan: ConnectionGroup = {
            connection_id: null,
            exchange_name: null,
            allocations: [],
            events: [],
          };
          for (const ev of events) {
            if (ev.connection_id && byConn[ev.connection_id]) {
              byConn[ev.connection_id].events.push(ev);
            } else {
              orphan.events.push(ev);
            }
          }
          if (orphan.events.length > 0) groups.push(orphan);

          return (
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {groups.map(grp => {
                const ex = grp.connection_id
                  ? exchanges.find(e => e.id === grp.connection_id)
                  : undefined;
                // Exchange.balance is 0 when not yet fetched; treat as null
                // so the UI shows "—" rather than "$0" during load.
                const bal = ex && ex.balance > 0
                  ? ex.balance
                  : (ex && ex.status === "active" ? 0 : null);
                // Filter events by the exchange's anchor: events before the
                // anchor aren't part of the tracked history. When no explicit
                // anchor set, show all.
                const anchorMs = ex?.principalAnchorAt
                  ? new Date(ex.principalAnchorAt).getTime()
                  : null;
                const filteredEvents = anchorMs !== null
                  ? grp.events.filter(e => new Date(e.event_at).getTime() >= anchorMs)
                  : grp.events;
                const filteredGroup = { ...grp, events: filteredEvents };
                return (
                  <ExchangeCapitalGroup
                    key={grp.connection_id || "orphan"}
                    group={filteredGroup}
                    exchangeBalance={bal}
                    exchangeAnchorAt={ex?.principalAnchorAt ?? null}
                    exchangeBaselineUsd={ex?.principalBaselineUsd ?? null}
                    onEditAnchor={setEditingAnchor}
                    onUpdateEvent={setEditingEvent}
                    onDeleteEvent={setConfirmDelete}
                    onRecordNew={grp.connection_id ? () => setCreatingForConnection(grp.connection_id!) : undefined}
                    onReset={grp.connection_id ? () => setConfirmReset(grp.connection_id!) : undefined}
                  />
                );
              })}
              {groups.length === 0 && (
                <div style={{
                  background: "var(--bg1)", border: "1px solid var(--line)",
                  borderRadius: 6, padding: "16px", fontSize: 10, color: "var(--t2)",
                }}>
                  No linked exchanges yet. Capital events appear once you link an exchange above.
                </div>
              )}
            </div>
          );
        })()
      )}

      {(creatingForConnection || editingEvent) && (() => {
        // On CREATE: scope the allocation dropdown to just those on the
        // initiating exchange's connection. On EDIT: show all the
        // operator's allocations (remap path for manual overrides).
        const modalAllocations = creatingForConnection
          ? instances
              .filter(i =>
                i.id && !i.id.startsWith("temp-")
                && i.connectionId === creatingForConnection,
              )
              .map(i => ({
                id: i.id,
                label: `${i.strategyName}${i.exchangeName ? ` · ${i.exchangeName}` : ""}`,
              }))
          : allocOptions;
        return (
          <CapitalEventModal
            initial={editingEvent}
            allocations={modalAllocations}
            submitting={submitting}
            onCancel={() => { setCreatingForConnection(null); setEditingEvent(null); }}
            onSubmit={data => {
              if (editingEvent) {
                handleEdit(editingEvent.event_id, {
                  amount_usd: data.amount_usd,
                  kind: data.kind,
                  event_at: data.event_at,
                  notes: data.notes,
                });
              } else {
                handleCreate({
                  ...data,
                  // Tie new events to the exchange panel they were initiated from
                  connection_id: creatingForConnection ?? undefined,
                });
              }
            }}
          />
        );
      })()}

      {confirmDelete && (
        <div
          role="dialog"
          aria-modal="true"
          onClick={() => { if (!submitting) setConfirmDelete(null); }}
          style={{
            position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              background: "var(--bg2)", border: "1px solid var(--line2)",
              borderRadius: 6, padding: "20px 24px",
              width: 420, maxWidth: "92vw", fontFamily: FONT_MONO,
            }}
          >
            <div style={{
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              color: "var(--t3)", textTransform: "uppercase", marginBottom: 10,
            }}>Delete capital event?</div>
            <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
              {fmtAmount(confirmDelete.amount_usd, confirmDelete.kind)} on {fmtCapitalDate(confirmDelete.event_at)}.
              {" "}This will be removed from PnL reconciliation immediately.
            </div>
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button
                type="button"
                disabled={submitting}
                onClick={() => setConfirmDelete(null)}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--t2)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >Cancel</button>
              <button
                type="button"
                disabled={submitting}
                onClick={() => handleDelete(confirmDelete.event_id)}
                style={{
                  background: "var(--red-dim)", border: "1px solid var(--red)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--red)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >Delete</button>
            </div>
          </div>
        </div>
      )}

      {/* Reset-to-default confirm (connection-scoped) */}
      {confirmReset && (() => {
        const exchangeName = exchanges.find(e => e.id === confirmReset)?.name
          ?? exchanges.find(e => e.id === confirmReset)?.exchange
          ?? confirmReset.slice(0, 8);
        return (
        <div
          role="dialog"
          aria-modal="true"
          onClick={() => { if (!submitting) setConfirmReset(null); }}
          style={{
            position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              background: "var(--bg2)", border: "1px solid var(--line2)",
              borderRadius: 6, padding: "20px 24px",
              width: 520, maxWidth: "92vw", fontFamily: FONT_MONO,
            }}
          >
            <div style={{
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              color: "var(--t3)", textTransform: "uppercase", marginBottom: 10,
            }}>Reset {exchangeName} capital events?</div>
            <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
              Delete all capital events on <span style={{ color: "var(--t0)" }}>{exchangeName}</span> —
              manual entries AND manual overrides on auto-detected rows — then re-sync from
              the exchange. Only events the exchange reports will remain. Principal on this
              exchange's allocations recomputes from exchange-truth values. Other exchanges
              are not affected. This cannot be undone.
            </div>
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button
                type="button"
                disabled={submitting}
                onClick={() => setConfirmReset(null)}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--t2)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >Cancel</button>
              <button
                type="button"
                disabled={submitting}
                onClick={() => handleReset(confirmReset)}
                style={{
                  background: "var(--amber-dim)", border: "1px solid var(--amber)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--amber)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >{submitting ? "Resetting…" : "Reset"}</button>
            </div>
          </div>
        </div>
        );
      })()}

      {/* Anchor-edit modal */}
      {editingAnchor && (() => {
        const ex = exchanges.find(e => e.id === editingAnchor);
        const exchangeLabel = ex ? (ex.name || ex.exchange) : editingAnchor.slice(0, 8);
        // Earliest event timestamp on this connection — used for the
        // "Set to earliest" button. events is already filtered to
        // deleted_at IS NULL by the list endpoint.
        const connEvents = (events ?? []).filter(ev => ev.connection_id === editingAnchor);
        const earliestEventAt = connEvents.length > 0
          ? connEvents.reduce((min, ev) => ev.event_at < min ? ev.event_at : min, connEvents[0].event_at)
          : null;
        return (
          <AnchorEditModal
            exchangeLabel={exchangeLabel}
            currentAnchorAt={ex?.principalAnchorAt ?? null}
            currentBaselineUsd={ex?.principalBaselineUsd ?? null}
            earliestEventAt={earliestEventAt}
            submitting={submitting}
            onCancel={() => setEditingAnchor(null)}
            onSave={data => handleAnchorSave(editingAnchor, data)}
          />
        );
      })()}
    </div>
  );
}

function AnchorEditModal({
  exchangeLabel,
  currentAnchorAt,
  currentBaselineUsd,
  earliestEventAt,
  submitting,
  onCancel,
  onSave,
}: {
  exchangeLabel: string;
  /** Operator-set anchor, or null to signal default. */
  currentAnchorAt: string | null;
  /** Current stored baseline — shown read-only as a hint but not editable
   * in the modal. Backend auto-derives on save from snapshot history. */
  currentBaselineUsd: number | null;
  /** ISO 8601 of the earliest recorded event on this connection, or null. */
  earliestEventAt: string | null;
  submitting: boolean;
  onCancel: () => void;
  onSave: (data: { anchor_at?: string; reset?: boolean }) => void;
}) {
  const [anchorAt, setAnchorAt] = useState<string>(
    currentAnchorAt ? currentAnchorAt.slice(0, 16) : "",
  );

  const hasOverride = currentAnchorAt !== null || currentBaselineUsd !== null;

  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={() => { if (!submitting) onCancel(); }}
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background: "var(--bg2)", border: "1px solid var(--line2)",
          borderRadius: 6, padding: "20px 24px",
          width: 480, maxWidth: "92vw", fontFamily: FONT_MONO,
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
        }}>
          Principal anchor — {exchangeLabel}
        </div>

        <div style={{ fontSize: 10, color: "var(--t2)", marginBottom: 14, lineHeight: 1.5 }}>
          Pin the official start date for this exchange's tracked history. Events and
          sessions before this date are excluded from principal math and displayed
          history. One anchor per exchange — shared across any allocations on the
          wallet. Leave blank to fall back to the connection creation date.
        </div>

        <div style={{ marginBottom: 10 }}>
          <div style={fieldLabelStyle}>Anchor date/time (local)</div>
          <input
            type="datetime-local"
            value={anchorAt}
            onChange={e => setAnchorAt(e.target.value)}
            style={inputStyle}
            onFocus={e => (e.target.style.borderColor = "var(--green)")}
            onBlur={e => (e.target.style.borderColor = "var(--line)")}
          />
          {earliestEventAt && (
            <button
              type="button"
              onClick={() => {
                // Convert UTC ISO to local datetime-local format (no TZ).
                const d = new Date(earliestEventAt);
                const pad = (n: number) => n.toString().padStart(2, "0");
                setAnchorAt(
                  `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
                  + `T${pad(d.getHours())}:${pad(d.getMinutes())}`
                );
              }}
              style={{
                marginTop: 6, background: "transparent", border: "none",
                color: "var(--t2)", fontSize: 9, letterSpacing: "0.08em",
                textTransform: "uppercase", padding: 0, cursor: "pointer",
                fontFamily: FONT_MONO,
              }}
              onMouseEnter={e => (e.currentTarget.style.color = "var(--green)")}
              onMouseLeave={e => (e.currentTarget.style.color = "var(--t2)")}
            >
              ↻ Set to earliest event date ({earliestEventAt.slice(0, 10)})
            </button>
          )}
        </div>

        <div style={{ marginBottom: 14 }}>
          <div style={fieldLabelStyle}>Baseline (auto-derived)</div>
          <div style={{
            padding: "10px 12px",
            background: "var(--bg3)",
            border: "1px solid var(--line)",
            borderRadius: 3,
            fontSize: 10, color: "var(--t2)",
            fontFamily: FONT_MONO,
          }}>
            {currentBaselineUsd !== null
              ? `$${currentBaselineUsd.toLocaleString("en-US", { maximumFractionDigits: 2 })}`
              : "—"}
          </div>
          <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4, lineHeight: 1.5 }}>
            Set automatically on save: the exchange wallet equity at the anchor moment,
            from snapshot history. If no snapshot exists before the anchor, baseline = 0.
          </div>
        </div>

        <div style={{ display: "flex", gap: 8, justifyContent: "space-between" }}>
          <button
            type="button"
            disabled={submitting || !hasOverride}
            onClick={() => onSave({ reset: true })}
            title={hasOverride
              ? "Clear the override and fall back to connection creation date + 0 baseline"
              : "No override to clear"}
            style={{
              background: "transparent", border: "1px solid var(--line2)",
              borderRadius: 4, padding: "8px 14px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: !hasOverride ? "var(--t3)" : "var(--amber)",
              cursor: (!hasOverride || submitting) ? "not-allowed" : "pointer",
              fontFamily: FONT_MONO,
            }}
          >Use default</button>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              disabled={submitting}
              onClick={onCancel}
              style={{
                background: "transparent", border: "1px solid var(--line2)",
                borderRadius: 4, padding: "8px 16px",
                fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                textTransform: "uppercase", color: "var(--t2)",
                cursor: submitting ? "not-allowed" : "pointer",
                fontFamily: FONT_MONO,
              }}
            >Cancel</button>
            <button
              type="button"
              disabled={submitting || !anchorAt}
              onClick={() => {
                try {
                  const iso = anchorAt ? new Date(anchorAt).toISOString() : undefined;
                  onSave({ anchor_at: iso });
                } catch { /* noop */ }
              }}
              style={(submitting || !anchorAt) ? disabledBtnStyle : primaryBtnStyle}
            >{submitting ? "Saving…" : "Save"}</button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function SettingsPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { exchanges, instances, removeExchange, loading, refresh } = useTrader();

  // Validate the openLink query param against the wizard's slug union.
  // Anything else (missing, "foo", etc.) yields null → no pre-fill.
  // Auto-open the wizard synchronously on mount when openLink is set
  // — using lazy useState init avoids the prior mount flicker where
  // the linked-exchanges list briefly rendered before a useEffect
  // flipped showWizard to true. Caller flow: legacy bookmarks or any
  // future deep-link to settings; /trader/get-started inlines its own
  // wizard and no longer routes here.
  const openLinkParam = searchParams.get("openLink");
  const initialExchange =
    openLinkParam === "blofin" || openLinkParam === "binance"
      ? openLinkParam
      : null;
  const [showWizard, setShowWizard] = useState(initialExchange !== null);
  const [removingId, setRemovingId] = useState<string | null>(null);
  const [blockedRemove, setBlockedRemove] = useState<string | null>(null);
  const confirm = useConfirm();

  async function handleWizardComplete() {
    setShowWizard(false);
    // Trigger a refresh so the new connection appears in the list with status=active.
    // Snapshot was already fetched inline by the backend; the next /snapshots call picks it up.
    try { await allocatorApi.refreshSnapshots(); } catch { /* non-fatal */ }
    await refresh();
    // If the user arrived via the get-started hero (?openLink=...),
    // route them to /trader/overview so the OnboardingNudge's "pick a
    // strategy" banner picks up the freshly-linked exchange. Without
    // this hop, the user is stranded on /trader/settings — the
    // (protected) layout's redirect doesn't fire (has_exchange is now
    // true) and the nudge doesn't render outside /trader/overview.
    if (searchParams.get("openLink")) {
      router.replace("/trader/overview");
    }
  }

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 640, margin: "0 auto" }}>

        {/* Existing exchanges — collapse when wizard open */}
        <div style={{ opacity: showWizard ? 0.4 : 1, transition: "opacity 0.2s ease" }}>
          <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginBottom: 12 }}>
            LINKED EXCHANGES
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: showWizard ? 4 : 10 }}>
            {exchanges.map(ex => showWizard ? (
              /* Minimal single-line format when wizard is open */
              <div key={ex.id} style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "7px 14px", display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 10, fontWeight: 700, color: "var(--t1)" }}>{ex.name}</span>
                <StatusPill status={ex.status} size="sm" />
                <span style={{ fontSize: 9, color: "var(--t2)" }}>{ex.maskedKey}</span>
              </div>
            ) : (
              /* Full expanded format */
              <div key={ex.id} style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 18px" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <span style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>{ex.name}</span>
                    <StatusPill status={ex.status} />
                  </div>
                  <button
                    onClick={async () => {
                      const liveOnExchange = instances.filter(i => i.exchangeId === ex.id && i.status === "live");
                      if (liveOnExchange.length > 0) {
                        setBlockedRemove(ex.id);
                        return;
                      }
                      if (ex.status === "active") {
                        // Non-destructive but in-use — require confirmation modal.
                        // Migrated from a bespoke ConfirmRemoveModal (deleted)
                        // to the shared ConfirmDialog primitive so every
                        // confirmation in the app uses one pattern.
                        const ok = await confirm({
                          eyebrow: "Settings · Confirm",
                          title: `Remove ${ex.name || ex.exchange} connection?`,
                          description: "This will revoke access to this exchange. Any traders using this connection will stop. You can re-link anytime.",
                          confirmLabel: "Remove",
                          destructive: true,
                        });
                        if (!ok) return;
                        setBlockedRemove(null);
                        setRemovingId(ex.id);
                        try {
                          await allocatorApi.removeExchange(ex.id);
                        } catch (err) {
                          console.error("removeExchange failed:", err);
                        } finally {
                          setRemovingId(null);
                        }
                        removeExchange(ex.id);
                        refresh();
                        return;
                      }
                      // Non-active row (errored / invalid / pending_validation) — single click, no confirm.
                      setRemovingId(ex.id);
                      try {
                        await allocatorApi.removeExchange(ex.id);
                      } catch (err) {
                        console.error("removeExchange failed:", err);
                      } finally {
                        setRemovingId(null);
                      }
                      removeExchange(ex.id);
                      refresh();
                    }}
                    disabled={removingId === ex.id}
                    onMouseEnter={e => { e.currentTarget.style.color = "var(--red)"; }}
                    onMouseLeave={e => { e.currentTarget.style.color = "var(--t3)"; }}
                    style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", cursor: removingId === ex.id ? "not-allowed" : "pointer", transition: "color 0.15s ease", opacity: removingId === ex.id ? 0.5 : 1 }}
                  >
                    {removingId === ex.id ? "REMOVING…" : "REMOVE"}
                  </button>
                </div>

                {/* Status-specific metadata line */}
                {ex.status === "active" && (
                  <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t1)" }}>
                    <span>{ex.maskedKey}</span>
                    <span style={{ color: "var(--t2)" }}>synced {ex.lastSynced}</span>
                    {ex.balance > 0 && (
                      <span style={{ color: "var(--t1)" }}>
                        ${ex.balance.toLocaleString("en-US", { maximumFractionDigits: 2 })}
                      </span>
                    )}
                  </div>
                )}

                {ex.status === "pending_validation" && (
                  <>
                    <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t2)" }}>
                      <span>{ex.maskedKey}</span>
                    </div>
                    <div style={{ fontSize: 10, color: "var(--amber)", marginTop: 4, lineHeight: 1.5 }}>
                      Awaiting first permissions check. This usually resolves within 5 minutes.
                    </div>
                  </>
                )}

                {ex.status === "invalid" && (
                  <>
                    <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t2)" }}>
                      <span>{ex.maskedKey}</span>
                    </div>
                    {ex.lastErrorMsg && (
                      <div style={{ fontSize: 10, color: "var(--red)", marginTop: 4, lineHeight: 1.5, opacity: 0.9 }}>
                        {ex.lastErrorMsg}
                      </div>
                    )}
                  </>
                )}

                {ex.status === "errored" && (
                  <>
                    <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t2)" }}>
                      <span>{ex.maskedKey}</span>
                    </div>
                    {ex.lastErrorMsg && (
                      <div style={{ fontSize: 10, color: "var(--amber)", marginTop: 4, lineHeight: 1.5, opacity: 0.9 }}>
                        {ex.lastErrorMsg}
                      </div>
                    )}
                    <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
                      We&rsquo;ll retry automatically in a few minutes.
                    </div>
                  </>
                )}

                {blockedRemove === ex.id && (() => {
                  const liveOnExchange = instances.filter(i => i.exchangeId === ex.id && i.status === "live");
                  return (
                    <div style={{
                      background: "var(--bg2)", border: "0.5px solid #ff4d4d30", borderRadius: 6,
                      padding: "10px 12px", marginTop: 10,
                    }}>
                      <div style={{ fontSize: 10, color: "var(--t2)", lineHeight: 1.6, marginBottom: 8 }}>
                        This exchange is used by {liveOnExchange.length} active trader{liveOnExchange.length > 1 ? "s" : ""}. Pause or remove them before unlinking.
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 8 }}>
                        {liveOnExchange.map(inst => (
                          <span
                            key={inst.id}
                            onClick={() => router.push(`/trader/traders/${inst.id}`)}
                            style={{ fontSize: 10, color: "var(--t1)", cursor: "pointer", transition: "color 0.15s ease" }}
                            onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; e.currentTarget.style.textDecoration = "underline"; }}
                            onMouseLeave={e => { e.currentTarget.style.color = "var(--t1)"; e.currentTarget.style.textDecoration = "none"; }}
                          >{inst.strategyName} &middot; {inst.exchangeName} &rarr;</span>
                        ))}
                      </div>
                      <button onClick={() => setBlockedRemove(null)} style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0 }}>Cancel</button>
                    </div>
                  );
                })()}
              </div>
            ))}
            {loading && (
              <div style={{ padding: "24px 0", textAlign: "center", color: "var(--t2)", fontSize: 10 }}>Loading exchanges...</div>
            )}
            {exchanges.length === 0 && !showWizard && !loading && (
              <div style={{ padding: "24px 0", textAlign: "center", color: "var(--t2)", fontSize: 10 }}>No exchanges linked yet.</div>
            )}
          </div>
        </div>

        {/* Link new exchange button / wizard */}
        {!showWizard ? (
          <button onClick={() => setShowWizard(true)} style={{
            marginTop: 14, width: "100%", padding: "10px 0",
            background: "transparent", color: "var(--green)",
            border: "1px dashed var(--line2)", borderRadius: 5,
            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer",
          }}>+ LINK A NEW EXCHANGE</button>
        ) : (
          <ExchangeLinkWizard
            initialExchange={initialExchange}
            onSuccess={() => handleWizardComplete()}
            onCancel={() => setShowWizard(false)}
          />
        )}

        {/* Exchange accounts summary table */}
        {exchanges.length > 0 && !showWizard && (
          <div style={{ marginTop: 20 }}>
            <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginBottom: 12 }}>
              ACCOUNT SUMMARY
            </div>
            <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6, overflow: "hidden" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid var(--line)" }}>
                    {["EXCHANGE", "BALANCE", "ALLOCATED", "TRADERS", "STATUS"].map(h => (
                      <th key={h} style={{ padding: "7px 14px", textAlign: "left", fontSize: 9, color: "var(--t3)", fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {exchanges.map((ex, idx) => {
                    const isActive = ex.status === "active";
                    const exInstances = instances.filter(i => i.exchangeId === ex.id && i.status === "live");
                    const exAllocated = exInstances.reduce((s, i) => s + (i.allocation ?? 0), 0);
                    const pctDeployed = ex.balance > 0 ? Math.min(100, (exAllocated / ex.balance) * 100).toFixed(1) : "0.0";
                    const dotColor = statusTheme(ex.status).color;
                    return (
                      <tr key={ex.id} style={{ borderBottom: idx < exchanges.length - 1 ? "1px solid var(--line)" : "none" }}>
                        <td style={{ padding: "10px 14px" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <span style={{ width: 5, height: 5, borderRadius: "50%", background: dotColor, flexShrink: 0 }} />
                            <span style={{ fontSize: 10, fontWeight: 700, color: "var(--t0)" }}>{ex.name}</span>
                          </div>
                        </td>
                        <td style={{ padding: "10px 14px" }}>
                          <div style={{ fontSize: 10, color: isActive ? "var(--t1)" : "var(--t3)" }}>
                            {isActive ? `$${ex.balance.toLocaleString("en-US")}` : "—"}
                          </div>
                        </td>
                        <td style={{ padding: "10px 14px" }}>
                          <div style={{ fontSize: 10, color: isActive ? "var(--t1)" : "var(--t3)" }}>
                            {isActive ? `$${exAllocated.toLocaleString("en-US")}` : "—"}
                          </div>
                          {isActive && (
                            <div style={{ fontSize: 9, color: "var(--t3)" }}>{exInstances.length} traders &middot; {pctDeployed}%</div>
                          )}
                        </td>
                        <td style={{ padding: "10px 14px", fontSize: 10, color: isActive ? "var(--t1)" : "var(--t3)" }}>
                          {isActive ? exInstances.length : "—"}
                        </td>
                        <td style={{ padding: "10px 14px" }}>
                          <StatusPill status={ex.status} />
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Capital events — operator-recorded deposits/withdrawals.
            Always rendered (regardless of wizard state) since it's a separate
            concern from exchange linking. */}
        {!showWizard && <CapitalEventsSection />}
      </div>

    </div>
  );
}

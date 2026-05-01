"use client";

/**
 * Manager → Live tab.
 *
 * Sections wired against the live BloFin connection (T0/T2 data only;
 * heavier vizes — coverage matrix, factor decomp, box plots, MA heatmap
 * — land in steps 8+):
 *   * Account Snapshot KPI strip (§3) ← /api/manager/live/account
 *   * Risk Signals row (§4)            ← /api/manager/live/risk
 *   * Position Map / Treemap (§5)      ← /api/manager/live/positions
 *   * PnL Attribution Waterfall (§6)   ← /api/manager/live/positions
 *   * Exposure Long vs Short (§7)      ← /api/manager/live/positions + /account
 *   * Open Positions Table (§12)       ← /api/manager/live/positions
 *
 * Polling cadence per Data Dictionary §1: 2s on /account + /positions,
 * 5s on /risk. Drops to 60s when document.hidden.
 *
 * Per-section error degradation: a failed endpoint shows a "STALE Ns"
 * tag in the section header and "—" in affected fields, but other
 * sections continue rendering. The page never blanks.
 *
 * Cross-viz interaction: clicking a treemap tile scrolls the matching
 * row in the Open Positions Table into view and pulses its background
 * (row-pulse keyframe in globals.css, 1.5s).
 */

import {
  CSSProperties,
  ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import Collapsible from "../../../components/Collapsible";
import Skeleton from "../../../components/Skeleton";
import { useLivePoll } from "../../../components/useLivePoll";
import Treemap from "./Treemap";
import Waterfall from "./Waterfall";
import ExposureMap from "./ExposureMap";
import type {
  AccountSnapshot,
  LivePosition,
  PositionsResponse,
  RiskSnapshot,
  Side,
  Source,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Format helpers ──────────────────────────────────────────────────────

function fmtUsd(n: number | null | undefined, signed = false): string {
  if (n === null || n === undefined) return "—";
  const sign = signed ? (n > 0 ? "+" : n < 0 ? "−" : "") : n < 0 ? "−" : "";
  const abs = Math.abs(n);
  const formatted = abs.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  return `${sign}$${formatted}`;
}

function fmtPct(n: number | null | undefined, signed = true): string {
  if (n === null || n === undefined) return "—";
  const sign = signed && n > 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
}

/** Smart-precision price formatting:
 *  ≥ 1000 → 2 decimals, comma thousands
 *  ≥ 1    → 4 decimals
 *  < 1    → 6 decimals (covers cents-and-below tokens)
 */
function fmtPrice(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  if (n >= 1000) {
    return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  }
  if (n >= 1) {
    return `$${n.toFixed(4)}`;
  }
  return `$${n.toFixed(6)}`;
}

function fmtSize(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return n.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

function pctColor(n: number | null | undefined): string {
  if (n === null || n === undefined) return "var(--t1)";
  if (n > 0) return "var(--green)";
  if (n < 0) return "var(--red)";
  return "var(--t1)";
}

/** Color the nearest-stop distance in red when within 2%, amber when
 *  within 5%, neutral beyond. Distance is the SIGNED bps between SL
 *  and mark — we color on absolute proximity. */
function stopProximityColor(distancePct: number | null): string {
  if (distancePct === null) return "var(--t1)";
  const abs = Math.abs(distancePct);
  if (abs < 2) return "var(--red)";
  if (abs < 5) return "var(--amber)";
  return "var(--t1)";
}

// ─── Tiny shared primitives ──────────────────────────────────────────────

function StaleTag({ ageS }: { ageS: number }) {
  return (
    <span
      style={{
        marginLeft: 8,
        padding: "2px 6px",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.1em",
        background: "var(--red-dim)",
        color: "var(--red)",
        border: "1px solid var(--red)",
        borderRadius: 2,
      }}
    >
      STALE {ageS}s
    </span>
  );
}

function staleSeconds(lastUpdatedAt: Date | null): number {
  if (!lastUpdatedAt) return 0;
  return Math.max(0, Math.floor((Date.now() - lastUpdatedAt.getTime()) / 1000));
}

// ─── Page header ─────────────────────────────────────────────────────────

interface HeaderProps {
  venue: string;
  positionsCount: number | null;
  lastUpdatedAt: Date | null;
  refreshing: boolean;
  onRefresh: () => void;
}

function PageHeader({ venue, positionsCount, lastUpdatedAt, refreshing, onRefresh }: HeaderProps) {
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const updatedLabel = lastUpdatedAt
    ? `UPDATED ${lastUpdatedAt.toISOString().slice(11, 19)} UTC`
    : "UPDATED —";

  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-end",
        justifyContent: "space-between",
        gap: 16,
        flexWrap: "wrap",
        marginBottom: 18,
      }}
    >
      <div>
        <div
          style={{
            fontSize: 22,
            fontWeight: 700,
            letterSpacing: "0.02em",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          Live Account Status
        </div>
        <div
          style={{
            display: "flex",
            gap: 14,
            alignItems: "center",
            color: "var(--t2)",
            fontSize: 11,
            letterSpacing: "0.06em",
            marginTop: 4,
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          <LiveTag />
          <span>VENUE · {venue.toUpperCase()}</span>
          <span>{positionsCount === null ? "— OPEN POSITIONS" : `${positionsCount} OPEN POSITIONS`}</span>
          <span>{updatedLabel}</span>
        </div>
      </div>

      <div style={{ display: "flex", gap: 8 }}>
        <HeaderButton onClick={() => { /* placeholder — out of v1 scope */ }}>
          EXPORT
        </HeaderButton>
        <FlattenAllButton />
        <HeaderButton onClick={onRefresh} primary disabled={refreshing}>
          {refreshing ? "REFRESHING…" : "REFRESH ↻"}
        </HeaderButton>
      </div>
    </div>
  );
}

function LiveTag() {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        color: "var(--green)",
        border: "1px solid var(--green)",
        padding: "3px 8px",
        borderRadius: 3,
        fontSize: 10,
        letterSpacing: "0.12em",
      }}
    >
      <span
        aria-hidden
        style={{
          width: 6,
          height: 6,
          borderRadius: "50%",
          background: "var(--green)",
          boxShadow: "0 0 6px rgba(0, 200, 150, 0.45)",
          animation: "pulse-dot 1.6s ease-in-out infinite",
        }}
      />
      LIVE · TICK 2s
    </span>
  );
}

function HeaderButton({
  children, onClick, primary, danger, disabled,
}: {
  children: ReactNode;
  onClick?: () => void;
  primary?: boolean;
  danger?: boolean;
  disabled?: boolean;
}) {
  const base: CSSProperties = {
    border: "1px solid var(--line)",
    padding: "6px 12px",
    borderRadius: 3,
    background: "transparent",
    color: "var(--t2)",
    fontSize: 11,
    letterSpacing: "0.08em",
    cursor: disabled ? "default" : "pointer",
    fontFamily: "var(--font-space-mono), Space Mono, monospace",
    opacity: disabled ? 0.5 : 1,
  };
  const variant: CSSProperties = primary
    ? { borderColor: "var(--green)", color: "var(--green)", background: "var(--green-dim)" }
    : danger
      ? { borderColor: "var(--red)", color: "var(--red)", background: "var(--red-dim)" }
      : {};
  return (
    <button onClick={onClick} disabled={disabled} style={{ ...base, ...variant }}>
      {children}
    </button>
  );
}

function FlattenAllButton() {
  const [progress, setProgress] = useState(0);
  const [active, setActive] = useState(false);

  const start = useCallback(() => setActive(true), []);
  const stop = useCallback(() => {
    setActive(false);
    setProgress(0);
  }, []);

  useEffect(() => {
    if (!active) return;
    const STEP_MS = 50;
    const TOTAL_MS = 2000;
    const id = setInterval(() => {
      setProgress((p) => {
        const next = p + STEP_MS;
        if (next >= TOTAL_MS) {
          clearInterval(id);
          // Reached "ready" — placeholder, no action in v1.
          setTimeout(() => {
            setActive(false);
            setProgress(0);
          }, 400);
          return TOTAL_MS;
        }
        return next;
      });
    }, STEP_MS);
    return () => clearInterval(id);
  }, [active]);

  const fillPct = active ? Math.min(100, (progress / 2000) * 100) : 0;

  return (
    <button
      onMouseDown={start}
      onMouseUp={stop}
      onMouseLeave={stop}
      onTouchStart={start}
      onTouchEnd={stop}
      title="Hold 2s to confirm — placeholder, no action wired in v1"
      style={{
        position: "relative",
        border: "1px solid var(--red)",
        padding: "6px 12px",
        borderRadius: 3,
        background: "var(--red-dim)",
        color: "var(--red)",
        fontSize: 11,
        letterSpacing: "0.08em",
        cursor: "pointer",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        overflow: "hidden",
      }}
    >
      <span
        aria-hidden
        style={{
          position: "absolute",
          inset: 0,
          background: "rgba(255, 77, 77, 0.25)",
          width: `${fillPct}%`,
          transition: active ? "none" : "width 200ms ease",
        }}
      />
      <span style={{ position: "relative" }}>
        {fillPct >= 100 ? "READY" : "⚠ FLATTEN ALL · HOLD"}
      </span>
    </button>
  );
}

// ─── KPI strip (§3) ──────────────────────────────────────────────────────

interface KpiCardProps {
  label: string;
  value: ReactNode;
  valueColor?: string;
  subLeft?: ReactNode;
  subRight?: ReactNode;
}

function KpiCard({ label, value, valueColor, subLeft, subRight }: KpiCardProps) {
  return (
    <div style={{ background: "var(--bg2)", padding: "14px 16px" }}>
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.14em",
          color: "var(--t3)",
          textTransform: "uppercase",
          marginBottom: 8,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 22,
          fontWeight: 700,
          color: valueColor ?? "var(--t0)",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          fontVariantNumeric: "tabular-nums",
          letterSpacing: "0.01em",
          lineHeight: 1.1,
        }}
      >
        {value}
      </div>
      {(subLeft || subRight) && (
        <div
          style={{
            marginTop: 8,
            fontSize: 11,
            color: "var(--t2)",
            display: "flex",
            justifyContent: "space-between",
            gap: 8,
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          <span>{subLeft}</span>
          {subRight && <span style={{ color: "var(--t3)" }}>{subRight}</span>}
        </div>
      )}
    </div>
  );
}

interface KpiStripProps {
  account: AccountSnapshot | null;
}

function KpiStrip({ account }: KpiStripProps) {
  if (!account) return <KpiStripSkeleton />;

  // Today's PnL color: green/red on sign, amber if anchor missing
  const todayColor = account.today_anchor_missing
    ? "var(--amber)"
    : pctColor(account.today_pnl_usd);
  const todaySub = account.today_anchor_missing
    ? "today_pnl unavailable"
    : `${fmtUsd(account.today_pnl_usd, true)} today`;

  // Notional > equity multiplier
  const notRatio = account.notional_to_equity;

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(6, 1fr)",
        gap: 1,
        background: "var(--line)",
        borderRadius: 4,
        overflow: "hidden",
        marginTop: 4,
      }}
    >
      <KpiCard
        label="Account Equity"
        value={fmtUsd(account.total_equity_usd)}
        subLeft={`cash ${fmtUsd(account.available_usd)}`}
        subRight={
          <span style={{ color: todayColor }}>{todaySub}</span>
        }
      />
      <KpiCard
        label="Deployed Margin"
        value={fmtUsd(account.used_margin_usd)}
        subLeft={`${account.used_margin_pct.toFixed(1)}% of equity`}
        subRight={`${account.open_position_count} positions`}
      />
      <KpiCard
        label="Total Notional"
        value={fmtUsd(account.total_notional_usd)}
        subLeft={`${notRatio.toFixed(2)}× equity`}
        subRight={`L ${fmtUsd(account.long_notional_usd)} / S ${fmtUsd(account.short_notional_usd)}`}
      />
      <KpiCard
        label="Net Unrealized PnL"
        value={fmtUsd(account.unrealized_pnl_usd, true)}
        valueColor={pctColor(account.unrealized_pnl_usd)}
        subLeft={
          <span style={{ color: pctColor(account.unrealized_pnl_pct) }}>
            {fmtPct(account.unrealized_pnl_pct)} on equity
          </span>
        }
        subRight={`${account.green_count} of ${account.open_position_count} green`}
      />
      <KpiCard
        label="Avg PnL / Position"
        value={fmtPct(account.avg_pnl_pct)}
        valueColor={pctColor(account.avg_pnl_pct)}
        subLeft={`median ${fmtPct(account.median_pnl_pct)}`}
        subRight={`σ ${account.pnl_pct_stdev.toFixed(2)}%`}
      />
      <KpiCard
        label="Avg Leverage"
        value={`${account.avg_leverage.toFixed(1)}×`}
        subLeft="weighted by notional"
        subRight={`range ${account.min_leverage.toFixed(1)}–${account.max_leverage.toFixed(1)}×`}
      />
    </div>
  );
}

function KpiStripSkeleton() {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(6, 1fr)",
        gap: 1,
        background: "var(--line)",
        borderRadius: 4,
        overflow: "hidden",
        marginTop: 4,
      }}
    >
      {Array.from({ length: 6 }).map((_, i) => (
        <div key={i} style={{ background: "var(--bg2)", padding: "14px 16px" }}>
          <Skeleton width={80} height={9} />
          <div style={{ height: 8 }} />
          <Skeleton width={120} height={20} />
          <div style={{ height: 8 }} />
          <Skeleton width={140} height={10} />
        </div>
      ))}
    </div>
  );
}

// ─── Risk signals (§4) ───────────────────────────────────────────────────

interface RiskCellProps {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  valueColor?: string;
}

function RiskCell({ label, value, detail, valueColor }: RiskCellProps) {
  return (
    // minWidth:0 lets a 1fr grid cell shrink below intrinsic content width;
    // overflow:hidden + overflowWrap on the inner texts keeps long
    // concatenated symbol lists inside the cell instead of bleeding into
    // the next section.
    <div style={{ background: "var(--bg2)", padding: "12px 16px", minWidth: 0, overflow: "hidden" }}>
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.14em",
          color: "var(--t3)",
          textTransform: "uppercase",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 14,
          fontWeight: 700,
          color: valueColor ?? "var(--t0)",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          fontVariantNumeric: "tabular-nums",
          overflowWrap: "break-word",
          wordBreak: "break-word",
        }}
      >
        {value}
      </div>
      {detail && (
        <div
          style={{
            marginTop: 4,
            fontSize: 10,
            color: "var(--t3)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            fontVariantNumeric: "tabular-nums",
            overflowWrap: "break-word",
            wordBreak: "break-word",
          }}
        >
          {detail}
        </div>
      )}
    </div>
  );
}

function RiskSignalsRow({ risk }: { risk: RiskSnapshot | null }) {
  if (!risk) return <RiskRowSkeleton />;

  // Margin level
  const mlValue = risk.margin_level.ratio !== null
    ? `${risk.margin_level.ratio.toFixed(1)}×`
    : "—";
  const mlDetail = risk.margin_level.ratio !== null && risk.margin_level.liquidation_buffer_pct !== null
    ? `liquidation buffer ${risk.margin_level.liquidation_buffer_pct.toFixed(1)}%`
    : (risk.margin_level.note ?? "");

  // Largest position
  const lp = risk.largest_position;

  // Nearest stop
  const ns = risk.nearest_stop;
  const nsValue = ns.symbol_base
    ? `${ns.symbol_base} · ${fmtPct(ns.distance_pct)} to SL`
    : "—";
  const nsColor = stopProximityColor(ns.distance_pct);
  const nsDetail = ns.sl_price !== null && ns.mark_price !== null
    ? `SL ${fmtPrice(ns.sl_price)} · mark ${fmtPrice(ns.mark_price)}`
    : "no SL set on any position";

  // Concentration. Show the top 3 symbols inline; surface a "+N more"
  // badge for the rest so the cell doesn't overflow when 6+ positions
  // share the concentrated direction. Full list lives in the no-stops
  // detail line and the positions table.
  const c = risk.concentration;
  const cTopSymbols = c ? c.constituent_symbols.slice(0, 3) : [];
  const cExtra = c ? c.constituent_symbols.length - cTopSymbols.length : 0;
  const cValue = c
    ? `${c.pct_of_book.toFixed(1)}% on ${cTopSymbols.join(" + ")}${cExtra > 0 ? ` +${cExtra}` : ""} ${c.direction}s`
    : "—";
  const cDetail = c && c.no_protective_stops.length > 0
    ? <span style={{ color: "var(--red)" }}>no protective stops on {c.no_protective_stops.slice(0, 3).join(", ")}{c.no_protective_stops.length > 3 ? ` +${c.no_protective_stops.length - 3}` : ""}</span>
    : (c ? "all positions hedged or stopped" : "");

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(4, 1fr)",
        gap: 1,
        background: "var(--line)",
        borderRadius: 4,
        overflow: "hidden",
        marginTop: 4,
      }}
    >
      <RiskCell label="Margin Level" value={mlValue} detail={mlDetail} />
      <RiskCell
        label="Largest Position"
        value={lp ? `${lp.symbol_base} · ${lp.notional_share_pct.toFixed(1)}% notional` : "—"}
        detail={lp ? `${fmtUsd(lp.notional_usd)} ${lp.side} · ${lp.leverage.toFixed(1)}× · ${lp.source}` : ""}
      />
      <RiskCell
        label="Nearest Stop"
        value={nsValue}
        valueColor={nsColor}
        detail={nsDetail}
      />
      <RiskCell
        label="Unhedged Concentration"
        value={cValue}
        detail={cDetail}
      />
    </div>
  );
}

function RiskRowSkeleton() {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(4, 1fr)",
        gap: 1,
        background: "var(--line)",
        borderRadius: 4,
        overflow: "hidden",
        marginTop: 4,
      }}
    >
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} style={{ background: "var(--bg2)", padding: "12px 16px" }}>
          <Skeleton width={80} height={9} />
          <div style={{ height: 6 }} />
          <Skeleton width={150} height={14} />
          <div style={{ height: 4 }} />
          <Skeleton width={180} height={9} />
        </div>
      ))}
    </div>
  );
}

// ─── Open Positions Table (§12) ──────────────────────────────────────────

type FilterKey = "all" | "strategy" | "manual" | "long" | "short";

function FilterChips({
  active,
  counts,
  onChange,
}: {
  active: FilterKey;
  counts: PositionsResponse["counts"];
  onChange: (k: FilterKey) => void;
}) {
  const chips: { key: FilterKey; label: string }[] = [
    { key: "all", label: `ALL · ${counts.total}` },
    { key: "strategy", label: `STRATEGY · ${counts.strategy}` },
    { key: "manual", label: `MANUAL · ${counts.manual}` },
    { key: "long", label: `LONG · ${counts.long}` },
    { key: "short", label: `SHORT · ${counts.short}` },
  ];
  return (
    <div style={{ display: "flex", gap: 4 }}>
      {chips.map((c) => {
        const isActive = c.key === active;
        return (
          <button
            key={c.key}
            onClick={(e) => {
              e.stopPropagation();
              onChange(c.key);
            }}
            style={{
              padding: "3px 9px",
              fontSize: 10,
              letterSpacing: "0.08em",
              border: "1px solid var(--line)",
              borderColor: isActive ? "var(--line2)" : "var(--line)",
              background: isActive ? "var(--bg3)" : "transparent",
              color: isActive ? "var(--t0)" : "var(--t2)",
              borderRadius: 3,
              cursor: "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            {c.label}
          </button>
        );
      })}
    </div>
  );
}

function SideTag({ side }: { side: Side }) {
  const isLong = side === "long";
  return (
    <span
      style={{
        padding: "2px 7px",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.14em",
        borderRadius: 2,
        color: isLong ? "var(--green)" : "var(--red)",
        background: isLong ? "var(--green-dim)" : "var(--red-dim)",
      }}
    >
      {isLong ? "LONG" : "SHORT"}
    </span>
  );
}

function SourceTag({ source, strategyName }: { source: Source; strategyName: string | null }) {
  const isStrat = source === "strategy";
  return (
    <span
      style={{
        padding: "2px 7px",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.12em",
        borderRadius: 2,
        border: "1px solid",
        color: isStrat ? "var(--green)" : "var(--amber)",
        borderColor: isStrat ? "var(--green)" : "var(--amber)",
      }}
    >
      {isStrat ? (strategyName ?? "STRATEGY").toUpperCase() : "MANUAL"}
    </span>
  );
}

function LevPill({ leverage }: { leverage: number | null }) {
  if (leverage === null) return <span style={{ color: "var(--t3)" }}>—</span>;
  const isHigh = leverage > 3;
  return (
    <span
      style={{
        display: "inline-block",
        padding: "1px 6px",
        fontSize: 10,
        border: "1px solid",
        borderColor: isHigh ? "var(--amber)" : "var(--line2)",
        color: isHigh ? "var(--amber)" : "var(--t2)",
        borderRadius: 2,
      }}
    >
      {leverage.toFixed(1)}×
    </span>
  );
}

/** Risk:reward fraction with a small horizontal split bar. The bar
 *  splits left (red, risk side) and right (green, reward side); the
 *  right portion's width = R/(R+1) so a 2.0 R:R fills 67% green. */
function RiskRewardCell({ rr }: { rr: number | null }) {
  if (rr === null) return <span style={{ color: "var(--t3)" }}>—</span>;
  const greenFrac = rr / (rr + 1);
  return (
    <div style={{ minWidth: 60 }}>
      <span style={{ color: rr >= 1 ? "var(--t1)" : "var(--red)" }}>
        {rr.toFixed(2)}
      </span>
      <div
        style={{
          width: 60,
          height: 4,
          background: "var(--line)",
          borderRadius: 2,
          overflow: "hidden",
          display: "flex",
          marginTop: 4,
        }}
      >
        <div style={{ width: `${(1 - greenFrac) * 100}%`, background: "var(--red)" }} />
        <div style={{ width: `${greenFrac * 100}%`, background: "var(--green)" }} />
      </div>
    </div>
  );
}

const tdStyle: CSSProperties = {
  fontSize: 11,
  color: "var(--t1)",
  padding: "11px 10px",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
  fontFamily: "var(--font-space-mono), Space Mono, monospace",
  fontVariantNumeric: "tabular-nums",
  verticalAlign: "middle",
};
const thStyle: CSSProperties = {
  textAlign: "left",
  padding: "10px 10px",
  fontWeight: 400,
  color: "var(--t3)",
  fontSize: 10,
  letterSpacing: "0.12em",
  borderBottom: "1px solid var(--line)",
  background: "var(--bg1)",
  whiteSpace: "nowrap",
};

function PositionRow({
  pos,
  expanded,
  onToggle,
  showExpanded,
  pulse,
}: {
  pos: LivePosition;
  expanded: boolean;
  onToggle: () => void;
  showExpanded: boolean;
  pulse?: boolean;
}) {
  const upl = pos.unrealized_pnl_usd;
  const uplPct = pos.unrealized_pnl_pct;
  const slDistColor = stopProximityColor(pos.sl_distance_pct);

  return (
    <>
      <tr
        data-pulse-symbol={pos.symbol}
        onClick={onToggle}
        className={pulse ? "row-pulse" : undefined}
        style={{
          cursor: "pointer",
          background: expanded ? "var(--bg1)" : "transparent",
        }}
      >
        <td style={tdStyle}>
          <div style={{ fontWeight: 700, color: "var(--t0)" }}>{pos.symbol_base}</div>
          <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.06em" }}>USDT-PERP</div>
        </td>
        <td style={{ ...tdStyle, textAlign: "center" }}>
          <SideTag side={pos.side} />
        </td>
        <td style={tdStyle}>
          <SourceTag source={pos.source} strategyName={pos.strategy_name} />
        </td>
        <td style={{ ...tdStyle, textAlign: "right" }}>{fmtPrice(pos.entry_price)}</td>
        <td style={{ ...tdStyle, textAlign: "right" }}>{fmtPrice(pos.mark_price)}</td>
        <td style={{ ...tdStyle, textAlign: "right" }}>{fmtSize(pos.size)}</td>
        <td style={{ ...tdStyle, textAlign: "right" }}>{fmtUsd(pos.notional_usd)}</td>
        <td style={{ ...tdStyle, textAlign: "center" }}>
          <LevPill leverage={pos.leverage} />
        </td>
        <td style={{ ...tdStyle, textAlign: "right" }}>
          <div style={{ fontWeight: 700, color: pctColor(upl) }}>{fmtUsd(upl, true)}</div>
          <div style={{ fontSize: 10, color: pctColor(uplPct) }}>{fmtPct(uplPct)}</div>
        </td>
        <td style={{ ...tdStyle, textAlign: "right", color: "var(--t2)" }}>
          {pos.age_seconds !== null
            ? fmtAge(pos.age_seconds)
            : <span style={{ color: "var(--t3)" }}>—</span>}
        </td>
        <td style={{ ...tdStyle, textAlign: "right" }}>
          {pos.sl_price !== null ? (
            <>
              <div style={{ color: "var(--red)" }}>{fmtPrice(pos.sl_price)}</div>
              <div style={{ fontSize: 9, color: slDistColor }}>{fmtPct(pos.sl_distance_pct)}</div>
            </>
          ) : (
            <span style={{ color: "var(--t3)", fontSize: 10 }}>— no SL</span>
          )}
        </td>
        <td style={{ ...tdStyle, textAlign: "right" }}>
          {pos.tp_price !== null ? (
            <>
              <div style={{ color: "var(--green)" }}>{fmtPrice(pos.tp_price)}</div>
              <div style={{ fontSize: 9, color: "var(--t3)" }}>{fmtPct(pos.tp_distance_pct)}</div>
            </>
          ) : (
            <span style={{ color: "var(--t3)", fontSize: 10 }}>— no TP</span>
          )}
        </td>
        <td style={{ ...tdStyle, textAlign: "right" }}>
          <RiskRewardCell rr={pos.risk_reward} />
        </td>
      </tr>
      {showExpanded && expanded && (
        <tr>
          <td
            colSpan={13}
            style={{
              padding: "16px 18px",
              background: "var(--bg1)",
              borderBottom: "1px solid var(--line2)",
              borderTop: "1px solid var(--line2)",
              fontSize: 11,
              color: "var(--t3)",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              letterSpacing: "0.06em",
            }}
          >
            Drill-down (execution history · market context · session
            context) loads in step 12.
          </td>
        </tr>
      )}
    </>
  );
}

function fmtAge(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${String(mins).padStart(2, "0")}m`;
  return `${mins}m`;
}

function OpenPositionsTable({
  data,
  pulseSymbol,
}: {
  data: PositionsResponse | null;
  pulseSymbol: string | null;
}) {
  const [filter, setFilter] = useState<FilterKey>("all");
  const [expandedSymbol, setExpandedSymbol] = useState<string | null>(null);

  const allPositions = data?.positions ?? [];
  const visible = useMemo(() => {
    if (filter === "all") return allPositions;
    if (filter === "strategy") return allPositions.filter((p) => p.source === "strategy");
    if (filter === "manual") return allPositions.filter((p) => p.source === "manual");
    if (filter === "long") return allPositions.filter((p) => p.side === "long");
    if (filter === "short") return allPositions.filter((p) => p.side === "short");
    return allPositions;
  }, [allPositions, filter]);

  if (!data) {
    return (
      <div style={{ padding: "12px 0" }}>
        <Skeleton width="100%" height={28} />
        <div style={{ height: 6 }} />
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} style={{ marginBottom: 6 }}>
            <Skeleton width="100%" height={32} />
          </div>
        ))}
      </div>
    );
  }

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 10 }}>
        <FilterChips active={filter} counts={data.counts} onChange={setFilter} />
      </div>
      <div
        style={{
          border: "1px solid var(--line)",
          borderRadius: 4,
          overflow: "auto",
          background: "var(--bg2)",
        }}
      >
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          <thead>
            <tr>
              <th style={thStyle}>SYMBOL</th>
              <th style={{ ...thStyle, textAlign: "center" }}>SIDE</th>
              <th style={thStyle}>SOURCE</th>
              <th style={{ ...thStyle, textAlign: "right" }}>ENTRY</th>
              <th style={{ ...thStyle, textAlign: "right" }}>MARK</th>
              <th style={{ ...thStyle, textAlign: "right" }}>SIZE</th>
              <th style={{ ...thStyle, textAlign: "right" }}>NOTIONAL</th>
              <th style={{ ...thStyle, textAlign: "center" }}>LEV</th>
              <th style={{ ...thStyle, textAlign: "right" }}>UNREALIZED PNL</th>
              <th style={{ ...thStyle, textAlign: "right" }}>AGE</th>
              <th style={{ ...thStyle, textAlign: "right" }}>SL · DIST</th>
              <th style={{ ...thStyle, textAlign: "right" }}>TP · DIST</th>
              <th style={{ ...thStyle, textAlign: "right" }}>R:R</th>
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 ? (
              <tr>
                <td colSpan={13} style={{ ...tdStyle, color: "var(--t3)", textAlign: "center", padding: "24px 10px" }}>
                  No positions match the current filter
                </td>
              </tr>
            ) : (
              visible.map((p) => (
                <PositionRow
                  key={`${p.connection_id}:${p.symbol}`}
                  pos={p}
                  expanded={expandedSymbol === p.symbol}
                  onToggle={() =>
                    setExpandedSymbol((v) => (v === p.symbol ? null : p.symbol))
                  }
                  showExpanded
                  pulse={pulseSymbol === p.symbol}
                />
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Right chat panel (read-only scaffold per step 5) ────────────────────

const CTX_CHIPS = [
  "OPEN POSITIONS",
  "90D ACCOUNT HISTORY",
  "LIVE FUNDING",
  "OI · L/S SKEW",
  "TA · 1m–1d",
  "STRATEGY METADATA",
];

function ChatPanel({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  return (
    <aside
      style={{
        width: collapsed ? 36 : 400,
        flexShrink: 0,
        borderLeft: "1px solid var(--line)",
        background: "var(--bg0)",
        position: "sticky",
        top: 0,
        height: "calc(100vh - 44px)",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        transition: "width 200ms ease",
      }}
    >
      <div
        style={{
          height: 40,
          borderBottom: "1px solid var(--line)",
          display: "flex",
          alignItems: "center",
          justifyContent: collapsed ? "center" : "space-between",
          padding: collapsed ? 0 : "0 12px",
          flexShrink: 0,
        }}
      >
        {!collapsed && (
          <span
            style={{
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "0.08em",
              color: "var(--t1)",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            PM <span style={{ color: "var(--purple, #7B5FFF)" }}>ASSISTANT</span>
          </span>
        )}
        <button
          onClick={onToggle}
          title={collapsed ? "Expand chat" : "Collapse chat"}
          style={{
            width: 24,
            height: 24,
            border: "1px solid var(--line2)",
            borderRadius: 3,
            background: "transparent",
            color: "var(--t1)",
            fontSize: 11,
            cursor: "pointer",
          }}
        >
          {collapsed ? "«" : "»"}
        </button>
      </div>

      {collapsed ? (
        <div
          style={{
            flex: 1,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            pointerEvents: "none",
          }}
        >
          <span
            style={{
              writingMode: "vertical-rl",
              transform: "rotate(180deg)",
              fontSize: 9,
              letterSpacing: "0.2em",
              color: "var(--t3)",
            }}
          >
            PM ASSISTANT · CTX LIVE
          </span>
        </div>
      ) : (
        <>
          <div
            style={{
              padding: "10px 12px",
              borderBottom: "1px solid var(--line)",
              display: "flex",
              flexWrap: "wrap",
              gap: 4,
              flexShrink: 0,
            }}
          >
            {CTX_CHIPS.map((c) => (
              <span
                key={c}
                style={{
                  fontSize: 9,
                  letterSpacing: "0.08em",
                  color: "var(--t3)",
                  padding: "3px 7px",
                  border: "1px solid var(--line)",
                  borderRadius: 2,
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 5,
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                }}
              >
                <span
                  aria-hidden
                  style={{
                    width: 5,
                    height: 5,
                    borderRadius: "50%",
                    background: "var(--green)",
                  }}
                />
                {c}
              </span>
            ))}
          </div>

          <div
            style={{
              flex: 1,
              overflowY: "auto",
              padding: "20px 14px",
              fontSize: 11,
              color: "var(--t3)",
              letterSpacing: "0.04em",
              textAlign: "center",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            Chat is read-only in v1. The PM-grade conversation engine
            ships in a separate task.
          </div>

          {/* Input — disabled in v1. No SUGGESTED prompts strip per
              design decision: chat panel goes straight from conversation
              thread to input. */}
          <div style={{ borderTop: "1px solid var(--line)", padding: "10px 12px 14px" }}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                background: "var(--bg2)",
                border: "1px solid var(--line2)",
                borderRadius: 3,
                padding: "0 10px",
              }}
            >
              <input
                disabled
                placeholder="Ask the PM…"
                style={{
                  flex: 1,
                  background: "transparent",
                  border: "none",
                  color: "var(--t2)",
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                  fontSize: 12,
                  padding: "10px 6px",
                  outline: "none",
                }}
              />
              <span style={{ color: "var(--t3)", fontSize: 11 }}>SEND ↵</span>
            </div>
          </div>
        </>
      )}
    </aside>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────

function Placeholder({ height = 80, label }: { height?: number; label?: string }) {
  return (
    <div
      style={{
        height,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "var(--t3)",
        fontSize: 10,
        letterSpacing: "0.06em",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {label ?? "Loading…"}
    </div>
  );
}

export default function LivePage() {
  const account = useLivePoll<AccountSnapshot>(`${API_BASE}/api/manager/live/account`, 2000);
  const risk = useLivePoll<RiskSnapshot>(`${API_BASE}/api/manager/live/risk`, 5000);
  const positions = useLivePoll<PositionsResponse>(`${API_BASE}/api/manager/live/positions`, 2000);

  const [chatCollapsed, setChatCollapsed] = useState(false);

  // Cross-viz interaction: clicking a treemap tile scrolls to + pulses
  // the matching positions-table row. State auto-clears after 1.5s
  // (matches the row-pulse animation duration in globals.css).
  const [pulseSymbol, setPulseSymbol] = useState<string | null>(null);
  const handleTileClick = useCallback((symbol: string) => {
    setPulseSymbol(symbol);
    if (typeof window !== "undefined") {
      window.requestAnimationFrame(() => {
        const row = document.querySelector(
          `[data-pulse-symbol="${CSS.escape(symbol)}"]`,
        );
        if (row instanceof HTMLElement) {
          row.scrollIntoView({ behavior: "smooth", block: "center" });
        }
      });
    }
    setTimeout(() => setPulseSymbol((cur) => (cur === symbol ? null : cur)), 1600);
  }, []);

  // Manual REFRESH ↻ — fires all three out-of-band fetches at once.
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const onRefresh = useCallback(async () => {
    setManualRefreshing(true);
    try {
      await Promise.all([account.refresh(), risk.refresh(), positions.refresh()]);
    } finally {
      setManualRefreshing(false);
    }
  }, [account, risk, positions]);

  const venue = account.data?.venue ?? "blofin";
  const positionsCount = positions.data?.counts.total ?? null;

  // Pick the most recent successful update across the three streams as
  // the page-level "UPDATED HH:MM:SS UTC" timestamp.
  const lastUpdatedAt = useMemo(() => {
    const times = [account.lastUpdatedAt, risk.lastUpdatedAt, positions.lastUpdatedAt]
      .filter((d): d is Date => d !== null);
    if (times.length === 0) return null;
    return new Date(Math.max(...times.map((d) => d.getTime())));
  }, [account.lastUpdatedAt, risk.lastUpdatedAt, positions.lastUpdatedAt]);

  // Per-section stale tags
  const accountStaleTag = account.isStale && (
    <StaleTag ageS={staleSeconds(account.lastUpdatedAt)} />
  );
  const riskStaleTag = risk.isStale && (
    <StaleTag ageS={staleSeconds(risk.lastUpdatedAt)} />
  );
  const positionsStaleTag = positions.isStale && (
    <StaleTag ageS={staleSeconds(positions.lastUpdatedAt)} />
  );

  // Collapsed-state summaries
  const accountSummary = account.data ? (
    <>
      EQUITY <strong style={{ color: "var(--t1)" }}>{fmtUsd(account.data.total_equity_usd)}</strong>
      {" · "}
      NET{" "}
      <strong style={{ color: pctColor(account.data.unrealized_pnl_usd) }}>
        {fmtUsd(account.data.unrealized_pnl_usd, true)}
      </strong>
      {" · "}
      DEPLOYED <strong style={{ color: "var(--t1)" }}>{fmtUsd(account.data.used_margin_usd)}</strong>
      {" · "}
      LEV <strong style={{ color: "var(--t1)" }}>{account.data.avg_leverage.toFixed(1)}×</strong>
    </>
  ) : "loading…";

  const riskSummary = risk.data ? (
    <>
      MARGIN{" "}
      <strong style={{ color: "var(--t1)" }}>
        {risk.data.margin_level.ratio !== null
          ? `${risk.data.margin_level.ratio.toFixed(1)}×`
          : "—"}
      </strong>
      {" · "}
      NEAREST STOP{" "}
      <strong style={{ color: stopProximityColor(risk.data.nearest_stop.distance_pct) }}>
        {risk.data.nearest_stop.symbol_base ?? "—"}{" "}
        {risk.data.nearest_stop.distance_pct !== null
          ? fmtPct(risk.data.nearest_stop.distance_pct)
          : ""}
      </strong>
      {risk.data.concentration && (
        <>
          {" · "}
          CONCENTRATION{" "}
          <strong style={{ color: "var(--t1)" }}>
            {risk.data.concentration.pct_of_book.toFixed(0)}% {risk.data.concentration.direction.toUpperCase()}
          </strong>
        </>
      )}
    </>
  ) : "loading…";

  const positionsSummary = positions.data ? (
    <>
      <strong style={{ color: "var(--t1)" }}>{positions.data.counts.total}</strong> positions
      {" · "}
      <span style={{ color: "var(--green)" }}>{positions.data.counts.strategy} strategy</span>
      {" · "}
      <span style={{ color: "var(--amber)" }}>{positions.data.counts.manual} manual</span>
    </>
  ) : "loading…";

  return (
    <div style={{ display: "flex", height: "100%" }}>
      <main style={{ flex: 1, overflow: "auto", padding: "16px 20px 32px" }}>
        <PageHeader
          venue={venue}
          positionsCount={positionsCount}
          lastUpdatedAt={lastUpdatedAt}
          refreshing={manualRefreshing}
          onRefresh={onRefresh}
        />

        <Collapsible
          id="live:account"
          title={<>Account Snapshot{accountStaleTag}</>}
          summary={accountSummary}
        >
          <KpiStrip account={account.data} />
        </Collapsible>

        <Collapsible
          id="live:risk"
          title={<>Risk Signals{riskStaleTag}</>}
          summary={riskSummary}
        >
          <RiskSignalsRow risk={risk.data} />
        </Collapsible>

        <Collapsible
          id="live:position-map"
          title="Position Map · Notional × PnL"
          summary={positions.data ? <>{positions.data.counts.total} POSITIONS</> : undefined}
        >
          <Treemap
            positions={positions.data?.positions ?? []}
            onTileClick={handleTileClick}
          />
        </Collapsible>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <Collapsible id="live:waterfall" title="PnL Attribution · Today">
            <Waterfall positions={positions.data?.positions ?? []} />
          </Collapsible>
          <Collapsible id="live:exposure" title="Exposure Composition · Live">
            <ExposureMap
              positions={positions.data?.positions ?? []}
              account={account.data}
            />
          </Collapsible>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <Collapsible id="live:coverage-matrix" title="Coverage Matrix · 30D Rolling">
            <Placeholder height={260} label="Pairwise correlation matrix renders in step 10" />
          </Collapsible>
          <Collapsible id="live:factor-decomp" title="Factor Decomposition · 30D Rolling">
            <Placeholder height={260} label="Effective-N + factor bar render in step 11" />
          </Collapsible>
        </div>

        <Collapsible id="live:box-plots" title="Trailing Distribution · 24H Window">
          <Placeholder height={170} label="Per-position box plots render in step 9" />
        </Collapsible>

        <Collapsible id="live:ma-heatmap" title="MA Alignment · Distance from EMA">
          <Placeholder height={250} label="EMA-distance heatmap renders in step 8" />
        </Collapsible>

        <Collapsible
          id="live:positions-table"
          title={
            <>
              Open Positions{positions.data ? ` · ${positions.data.counts.total}` : ""}
              {positionsStaleTag}
            </>
          }
          summary={positionsSummary}
        >
          <OpenPositionsTable data={positions.data} pulseSymbol={pulseSymbol} />
        </Collapsible>
      </main>

      <ChatPanel collapsed={chatCollapsed} onToggle={() => setChatCollapsed((v) => !v)} />
    </div>
  );
}

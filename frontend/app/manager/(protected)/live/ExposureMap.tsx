"use client";

/**
 * frontend/app/manager/(protected)/live/ExposureMap.tsx
 * ========================================================
 * Exposure Composition · Live (Data Dictionary §7, beta-weighted).
 *
 * Two views toggleable via the chip group at top-right:
 *
 *   * BTC-EQ (default): bar segments size by |β_BTC × notional|, the
 *     position's BTC-equivalent dollar exposure. NEIRO at β_BTC=2.13x
 *     looks ~2× larger than its raw notional; NAORIS at β_BTC≈0.4x
 *     compresses. This is the honest read for a long-only book where
 *     positions correlate to BTC at varying degrees — a $2k SOL long
 *     carries more BTC-driven risk than a $2k NAORIS long, and the
 *     viz now shows that.
 *   * NOTIONAL: the previous behavior — bar segments by raw notional.
 *
 * View choice persists in localStorage. While the factor decomposition
 * endpoint is still loading, BTC-EQ falls back to NOTIONAL with a small
 * "BTC-EQ pending…" note.
 *
 * Stat strip (4 tiles):
 *   * TOTAL EXPOSURE — position count
 *   * BTC-EQ EXPOSURE — Σ β_BTC × notional (the headline risk number)
 *   * LEVERAGE RATIO — BTC-eq / equity, color-tiered (<1.5× green,
 *     <3× amber, ≥3× red)
 *   * LEVERAGE Δ — notional_leverage − btc_eq_leverage. Tells the user
 *     whether their headline notional under- or over-states real BTC
 *     exposure. Sign-aware: green when BTC-eq < notional (less risk
 *     than headline suggests), red when BTC-eq > notional.
 *
 * Per-position table: adds β_BTC and BTC-eq columns. Sort key matches
 * the active view — BTC-eq desc in BTC-EQ view, notional desc in
 * NOTIONAL view.
 *
 * Edge cases:
 *   * |β_BTC| < 0.05 → label "≈0β" badge in the table; segment lands
 *     at the right of the bar in BTC-EQ view (segments are sorted
 *     |β_BTC × notional| desc, so near-zero β naturally tails).
 *   * β_BTC < 0 (rare on long-only crypto, but possible if regression
 *     returns one) → segment renders amber-tinted with a left-pointing
 *     border accent and a tooltip explaining the negative β.
 *   * Defensive HEDGES callout for short positions stays from the
 *     prior version — long-only assumption preserved.
 */

import { useEffect, useState } from "react";
import type {
  AccountSnapshot,
  FactorDecompositionResponse,
  FactorPositionRow,
  LivePosition,
} from "./types";

const LABEL_VISIBLE_COUNT = 3;
const STORAGE_KEY = "live:exposure-view";
const ZERO_BETA_FLOOR = 0.05;

type View = "btc-eq" | "notional";

interface Props {
  positions: LivePosition[];
  account: AccountSnapshot | null;
  factor: FactorDecompositionResponse | null;
}

type EnrichedPosition = LivePosition & {
  // Unitless BTC sensitivity ratio: $-β-from-API / notional. This is
  // what the user sees in the β column ("2.13×"). The dollar β from
  // the API already represents BTC-equivalent dollar exposure
  // directly — no further multiplication needed for the BTC-eq view.
  beta_btc_unitless: number | null;
  btc_eq_usd: number;          // |API β_BTC|; 0 when β unknown
  btc_eq_signed_usd: number;   // API β_BTC (signed); is the BTC-eq itself
  has_factor: boolean;         // true when factor row resolved with history
};

function useView(): [View, (v: View) => void] {
  const [view, setView] = useState<View>("btc-eq");
  // Hydrate from localStorage on mount; SSR-safe via the typeof window guard.
  useEffect(() => {
    if (typeof window === "undefined") return;
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "btc-eq" || stored === "notional") {
      setView(stored);
    }
  }, []);
  const persistAndSet = (v: View) => {
    setView(v);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(STORAGE_KEY, v);
    }
  };
  return [view, persistAndSet];
}

function enrichPositions(
  positions: LivePosition[],
  factorRows: FactorPositionRow[] | null,
): EnrichedPosition[] {
  const factorMap = new Map<string, FactorPositionRow>();
  for (const r of factorRows ?? []) {
    factorMap.set(r.symbol, r);
  }
  return positions.map((p) => {
    const fr = factorMap.get(p.symbol);
    const betaDollars = fr?.beta_btc ?? null;  // API: $-PnL per +100% BTC ret
    const has_factor = fr?.has_history === true && betaDollars !== null;
    // BTC-equivalent dollar exposure IS the API's β (already in $).
    // The unitless ratio for the β column is β_dollars / notional.
    const signedBtcEq = has_factor && betaDollars !== null ? betaDollars : 0;
    const unitless =
      has_factor && betaDollars !== null && p.notional_usd > 0
        ? betaDollars / p.notional_usd
        : null;
    return {
      ...p,
      beta_btc_unitless: unitless,
      btc_eq_signed_usd: signedBtcEq,
      btc_eq_usd: Math.abs(signedBtcEq),
      has_factor,
    };
  });
}

export default function ExposureMap({ positions, account, factor }: Props) {
  const [viewPref, setView] = useView();

  // BTC-EQ view falls back to NOTIONAL if the factor endpoint hasn't
  // landed yet. We still surface the chip toggle so the user knows it's
  // pending, not broken.
  const factorPending = factor === null && positions.length > 0;
  const factorReady = factor?.portfolio !== null && factor?.portfolio !== undefined;
  const view: View =
    viewPref === "btc-eq" && !factorReady ? "notional" : viewPref;

  const enriched = enrichPositions(positions, factor?.positions ?? null);
  const longs = enriched.filter((p) => p.side === "long");
  const shorts = enriched.filter((p) => p.side === "short");

  // Sort by current view's primary metric.
  const sortedLongs = [...longs].sort((a, b) =>
    view === "btc-eq"
      ? b.btc_eq_usd - a.btc_eq_usd
      : b.notional_usd - a.notional_usd,
  );

  const totalNotional = enriched.reduce((acc, p) => acc + p.notional_usd, 0);
  const totalLongNotional = longs.reduce((acc, p) => acc + p.notional_usd, 0);
  const totalShortNotional = shorts.reduce((acc, p) => acc + p.notional_usd, 0);
  const totalBtcEqSigned = enriched.reduce(
    (acc, p) => acc + p.btc_eq_signed_usd, 0,
  );
  const totalBtcEqAbs = enriched.reduce((acc, p) => acc + p.btc_eq_usd, 0);
  const equity = account?.total_equity_usd ?? 0;
  const notionalLev = equity > 0 ? totalNotional / equity : 0;
  const btcEqLev = equity > 0 ? Math.abs(totalBtcEqSigned) / equity : 0;
  // Delta: notional − btc_eq. If positive, headline overstates BTC risk
  // (book is more diversified than notional implies). If negative, BTC
  // exposure exceeds notional — common when high-β alts dominate.
  const leverageDelta = notionalLev - btcEqLev;

  // Headline number for the top-right "leverage ratio" tile follows
  // the active view: BTC-EQ in BTC-EQ view, notional in NOTIONAL view.
  const activeLev = view === "btc-eq" ? btcEqLev : notionalLev;

  if (positions.length === 0) {
    return (
      <div
        style={{
          height: 200,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--t3)",
          fontSize: 11,
          letterSpacing: "0.06em",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        No exposure
      </div>
    );
  }

  // Top-N visible labels by the active view's sizing.
  const labelVisible = new Set(
    sortedLongs.slice(0, LABEL_VISIBLE_COUNT).map((p) => p.symbol),
  );

  // For the bar: in BTC-EQ view, use signed contributions so the rare
  // negative-β segment can render with its own visual treatment. In
  // NOTIONAL view, all segments use raw notional (always positive).
  const barTotal =
    view === "btc-eq"
      ? sortedLongs.reduce((acc, p) => acc + p.btc_eq_usd, 0)
      : totalLongNotional;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      {/* Toggle + pending banner */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
        }}
      >
        <div
          style={{
            fontSize: 9,
            letterSpacing: "0.14em",
            color: "var(--t3)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          {view === "btc-eq"
            ? "BTC-EQUIVALENT EXPOSURE · WEIGHTED BY β_BTC"
            : "NOTIONAL EXPOSURE · RAW $"}
          {factorPending && viewPref === "btc-eq" && (
            <span style={{ color: "var(--amber)", marginLeft: 10 }}>
              · BTC-EQ pending…
            </span>
          )}
        </div>
        <ViewToggle view={viewPref} onChange={setView} factorReady={factorReady} />
      </div>

      {/* Main bar */}
      <div>
        <div
          style={{
            position: "relative",
            height: 38,
            display: "flex",
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 2,
            overflow: "hidden",
          }}
        >
          {sortedLongs.length === 0 ? (
            <div
              style={{
                flex: 1,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                color: "var(--t3)",
                fontSize: 10,
                letterSpacing: "0.08em",
              }}
            >
              long book empty
            </div>
          ) : (
            sortedLongs.map((p) => {
              const sizeMetric =
                view === "btc-eq" ? p.btc_eq_usd : p.notional_usd;
              const widthPct = barTotal > 0 ? (sizeMetric / barTotal) * 100 : 0;
              return (
                <Segment
                  key={p.symbol}
                  pos={p}
                  widthPct={widthPct}
                  showLabel={labelVisible.has(p.symbol)}
                  view={view}
                />
              );
            })
          )}
        </div>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            marginTop: 4,
            fontSize: 9,
            color: "var(--t3)",
            letterSpacing: "0.08em",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          <span>0</span>
          <span style={{ color: "var(--green)" }}>
            {view === "btc-eq"
              ? `$${totalBtcEqAbs.toLocaleString(undefined, { maximumFractionDigits: 0 })} BTC-EQ`
              : `$${totalLongNotional.toLocaleString(undefined, { maximumFractionDigits: 0 })} LONG`}
          </span>
        </div>
      </div>

      {/* Defensive HEDGES callout — only renders when shorts exist */}
      {shorts.length > 0 && (
        <div
          style={{
            background: "var(--bg2)",
            border: "1px solid var(--red)",
            borderLeft: "2px solid var(--red)",
            borderRadius: 3,
            padding: "8px 12px",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          <div
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.14em",
              color: "var(--red)",
              marginBottom: 6,
            }}
          >
            HEDGES · ${totalShortNotional.toLocaleString(undefined, { maximumFractionDigits: 0 })}
          </div>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: "4px 14px",
              fontSize: 10,
              color: "var(--t1)",
              fontVariantNumeric: "tabular-nums",
            }}
          >
            {shorts.map((p) => (
              <span key={p.symbol}>
                <span style={{ color: "var(--t0)", fontWeight: 700 }}>{p.symbol_base}</span>
                {" "}
                <span style={{ color: "var(--t3)" }}>
                  ${p.notional_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  {p.leverage ? ` · ${p.leverage.toFixed(1)}×` : ""}
                </span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Stat strip — 4 tiles, BTC-eq lives here whether or not the
          bar is in BTC-EQ view (the user always sees the headline
          BTC-equivalent risk number; only the bar's sizing changes). */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: 1,
          background: "var(--line)",
          border: "1px solid var(--line)",
          borderRadius: 3,
          overflow: "hidden",
        }}
      >
        <Stat
          label="Total Exposure"
          value={`${positions.length} position${positions.length === 1 ? "" : "s"}`}
          valueColor="var(--t0)"
          sub={
            shorts.length > 0
              ? `${longs.length} long · ${shorts.length} short`
              : `${longs.length} long`
          }
        />
        <Stat
          label="BTC-EQ Exposure"
          value={
            factorReady
              ? `$${totalBtcEqAbs.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
              : "—"
          }
          valueColor="var(--t0)"
          sub={
            factorReady
              ? `notional $${totalNotional.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
              : factorPending
                ? "computing…"
                : "no factor history"
          }
        />
        <Stat
          label="Leverage Ratio"
          value={`${activeLev.toFixed(2)}× equity`}
          valueColor={
            activeLev < 1.5
              ? "var(--green)"
              : activeLev < 3
                ? "var(--amber)"
                : "var(--red)"
          }
          sub={
            view === "btc-eq" && factorReady
              ? `BTC-eq vs notional ${notionalLev.toFixed(2)}×`
              : `equity $${equity.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
          }
        />
        <Stat
          label="Leverage Δ"
          value={
            factorReady
              ? `${leverageDelta >= 0 ? "+" : "−"}${Math.abs(leverageDelta).toFixed(2)}×`
              : "—"
          }
          // Convention: notional − btc_eq. Positive → BTC-eq is LOWER
          // than notional → headline overstates risk → green/safer.
          // Negative → BTC-eq is HIGHER than notional → red/more risk.
          valueColor={
            !factorReady
              ? "var(--t2)"
              : leverageDelta >= 0
                ? "var(--green)"
                : "var(--red)"
          }
          sub={
            !factorReady
              ? ""
              : leverageDelta >= 0
                ? "headline overstates BTC risk"
                : "headline understates BTC risk"
          }
        />
      </div>

      {/* Per-position breakdown — adds β + BTC-eq columns */}
      <div
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 3,
          padding: "10px 12px",
        }}
      >
        <div
          style={{
            fontSize: 9,
            letterSpacing: "0.16em",
            color: "var(--t3)",
            display: "flex",
            justifyContent: "space-between",
            marginBottom: 8,
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          <span>POSITIONS · LONG</span>
          <span style={{ color: "var(--green)", fontWeight: 700 }}>
            {view === "btc-eq" && factorReady
              ? `$${totalBtcEqAbs.toLocaleString(undefined, { maximumFractionDigits: 0 })} BTC-EQ`
              : `$${totalLongNotional.toLocaleString(undefined, { maximumFractionDigits: 0 })} TOTAL`}
          </span>
        </div>
        {/* Header row */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "90px 1fr 80px 70px 90px 60px",
            fontSize: 9,
            letterSpacing: "0.1em",
            color: "var(--t3)",
            paddingBottom: 4,
            borderBottom: "1px solid var(--line)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            gap: 12,
          }}
        >
          <span>SYMBOL</span>
          <span>SOURCE</span>
          <span style={{ textAlign: "right" }}>NOTIONAL</span>
          <span style={{ textAlign: "right" }}>β BTC</span>
          <span style={{ textAlign: "right" }}>BTC-EQ</span>
          <span style={{ textAlign: "right" }}>LEV</span>
        </div>
        {sortedLongs.length === 0 ? (
          <div style={{ fontSize: 10, color: "var(--t3)" }}>none</div>
        ) : (
          sortedLongs.map((p, i) => (
            <div
              key={p.symbol}
              style={{
                display: "grid",
                gridTemplateColumns: "90px 1fr 80px 70px 90px 60px",
                fontSize: 11,
                padding: "4px 0",
                borderBottom: i === sortedLongs.length - 1 ? "none" : "1px dashed var(--line)",
                fontVariantNumeric: "tabular-nums",
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
                gap: 12,
                alignItems: "center",
              }}
            >
              <span style={{ color: "var(--t0)", fontWeight: 700 }}>{p.symbol_base}</span>
              <span style={{ color: "var(--t3)", fontSize: 10 }}>
                {p.source === "strategy" ? (p.strategy_name ?? "STRATEGY") : "MANUAL"}
              </span>
              <span style={{ color: "var(--t1)", textAlign: "right" }}>
                ${p.notional_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}
              </span>
              <BetaCell beta={p.beta_btc_unitless} hasFactor={p.has_factor} />
              <BtcEqCell
                signed={p.btc_eq_signed_usd}
                hasFactor={p.has_factor}
              />
              <span style={{ color: "var(--t2)", textAlign: "right" }}>
                {p.leverage ? `${p.leverage.toFixed(1)}×` : "—"}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function ViewToggle({
  view, onChange, factorReady,
}: {
  view: View;
  onChange: (v: View) => void;
  factorReady: boolean;
}) {
  return (
    <div
      role="tablist"
      style={{
        display: "inline-flex",
        border: "1px solid var(--line)",
        borderRadius: 3,
        overflow: "hidden",
        fontSize: 10,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      <ToggleChip
        label="BTC-EQ"
        active={view === "btc-eq"}
        disabled={!factorReady}
        onClick={() => onChange("btc-eq")}
      />
      <ToggleChip
        label="NOTIONAL"
        active={view === "notional"}
        onClick={() => onChange("notional")}
      />
    </div>
  );
}

function ToggleChip({
  label, active, disabled, onClick,
}: {
  label: string;
  active: boolean;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={disabled ? undefined : onClick}
      disabled={disabled}
      style={{
        background: active ? "var(--green-mid)" : "var(--bg2)",
        color: active
          ? "var(--green)"
          : disabled
            ? "var(--t3)"
            : "var(--t1)",
        fontWeight: active ? 700 : 400,
        padding: "5px 10px",
        border: "none",
        cursor: disabled ? "not-allowed" : "pointer",
        letterSpacing: "0.1em",
        fontFamily: "inherit",
        fontSize: "inherit",
        opacity: disabled ? 0.55 : 1,
      }}
    >
      {label}
    </button>
  );
}

function Segment({
  pos, widthPct, showLabel, view,
}: {
  pos: EnrichedPosition;
  widthPct: number;
  showLabel: boolean;
  view: View;
}) {
  const negativeBeta =
    view === "btc-eq" && pos.has_factor && (pos.beta_btc_unitless ?? 0) < 0;
  const nearZeroBeta =
    view === "btc-eq" && pos.has_factor &&
    Math.abs(pos.beta_btc_unitless ?? 0) < ZERO_BETA_FLOOR;

  const tooltip =
    view === "btc-eq" && pos.has_factor && pos.beta_btc_unitless !== null
      ? `${pos.symbol_base} · β=${pos.beta_btc_unitless.toFixed(2)}× · ` +
        `BTC-eq $${pos.btc_eq_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}` +
        (negativeBeta ? " · negative β (counter-correlated to BTC)" : "")
      : `${pos.symbol_base} · $${pos.notional_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}` +
        (pos.leverage ? ` · ${pos.leverage.toFixed(1)}×` : "");

  // Visual treatment:
  //   * Default: green tint
  //   * Negative β in BTC-EQ view: amber tint with left border accent
  //   * Near-zero β: muted green tint, smaller label opacity (segment
  //     usually too small to label anyway since width ∝ |β × notional|)
  const bg = negativeBeta
    ? "rgba(240, 165, 0, 0.32)"
    : nearZeroBeta
      ? "rgba(0, 200, 150, 0.18)"
      : "rgba(0, 200, 150, 0.32)";
  const labelColor = negativeBeta ? "var(--amber)" : "var(--green)";

  return (
    <div
      title={tooltip}
      style={{
        height: "100%",
        width: `${widthPct}%`,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        color: labelColor,
        background: bg,
        borderRight: "1px solid rgba(8, 8, 9, 0.4)",
        borderLeft: negativeBeta ? "2px solid var(--amber)" : "none",
        overflow: "hidden",
        whiteSpace: "nowrap",
        transition: "width 280ms ease",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {showLabel ? pos.symbol_base : ""}
    </div>
  );
}

function BetaCell({ beta, hasFactor }: { beta: number | null; hasFactor: boolean }) {
  if (!hasFactor || beta === null) {
    return <span style={{ color: "var(--t3)", textAlign: "right" }}>—</span>;
  }
  const nearZero = Math.abs(beta) < ZERO_BETA_FLOOR;
  const negative = beta < 0;
  const color = nearZero
    ? "var(--t3)"
    : negative
      ? "var(--amber)"
      : "var(--t1)";
  return (
    <span
      style={{
        color,
        textAlign: "right",
        fontVariantNumeric: "tabular-nums",
      }}
      title={
        nearZero
          ? "below regression noise floor (|β| < 0.05)"
          : negative
            ? "negative β — counter-correlated to BTC"
            : undefined
      }
    >
      {nearZero ? "≈0β" : `${beta >= 0 ? "+" : "−"}${Math.abs(beta).toFixed(2)}×`}
    </span>
  );
}

function BtcEqCell({
  signed, hasFactor,
}: { signed: number; hasFactor: boolean }) {
  if (!hasFactor) {
    return <span style={{ color: "var(--t3)", textAlign: "right" }}>—</span>;
  }
  const abs = Math.abs(signed);
  const color = signed < 0 ? "var(--amber)" : "var(--t1)";
  return (
    <span
      style={{
        color,
        textAlign: "right",
        fontVariantNumeric: "tabular-nums",
      }}
    >
      {signed < 0 ? "−" : ""}${abs.toLocaleString(undefined, { maximumFractionDigits: 0 })}
    </span>
  );
}

function Stat({
  label, value, valueColor, sub,
}: {
  label: string;
  value: string;
  valueColor?: string;
  sub: string;
}) {
  return (
    <div style={{ background: "var(--bg2)", padding: "10px 12px" }}>
      <div
        style={{
          fontSize: 9,
          letterSpacing: "0.14em",
          color: "var(--t3)",
          marginBottom: 4,
          textTransform: "uppercase",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 16,
          fontWeight: 700,
          color: valueColor ?? "var(--t0)",
          fontVariantNumeric: "tabular-nums",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        {value}
      </div>
      <div
        style={{
          fontSize: 10,
          color: "var(--t3)",
          marginTop: 2,
          fontVariantNumeric: "tabular-nums",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        {sub}
      </div>
    </div>
  );
}

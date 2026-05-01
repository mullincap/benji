"use client";

/**
 * frontend/app/manager/(protected)/live/ExposureMap.tsx
 * ========================================================
 * Exposure Composition · Live (Data Dictionary §7, adapted for the
 * long-only book this account actually trades).
 *
 * The original §7 long-vs-short divergent bar wastes half the bar zone
 * when the book is single-sided. This component instead renders a
 * single full-width bar segmented by position, sized proportionally to
 * each position's share of total notional. Stat strip switches from
 * LONG/SHORT/NET to TOTAL/NOTIONAL/LEVERAGE-RATIO.
 *
 * Defensive: if a short position appears (manual hedge or strategy
 * extension), it renders in a separate "HEDGES" callout below the
 * main bar — the long-only composition view stays intact.
 *
 * Label clipping: top 3 positions by notional get visible ticker
 * labels in their segment; smaller segments stay unlabeled so the bar
 * doesn't read as overflowing text. The full ticker is available on
 * hover via the title attribute and in the breakdown list below.
 *
 * Future (post step 11 once factor decomposition lands per-position
 * BTC betas): replace notional-weighting with beta-weighted exposure
 * so the bar reflects effective BTC-equivalent risk rather than raw
 * dollar notional. Tracking item logged.
 */

import type { LivePosition, AccountSnapshot } from "./types";

interface Props {
  positions: LivePosition[];
  account: AccountSnapshot | null;
}

const LABEL_VISIBLE_COUNT = 3;

export default function ExposureMap({ positions, account }: Props) {
  const longs = [...positions]
    .filter((p) => p.side === "long")
    .sort((a, b) => b.notional_usd - a.notional_usd);
  const shorts = [...positions]
    .filter((p) => p.side === "short")
    .sort((a, b) => b.notional_usd - a.notional_usd);

  const totalLong = longs.reduce((acc, p) => acc + p.notional_usd, 0);
  const totalShort = shorts.reduce((acc, p) => acc + p.notional_usd, 0);
  const totalNotional = totalLong + totalShort;
  const equity = account?.total_equity_usd ?? 0;
  const leverageRatio = equity > 0 ? totalNotional / equity : 0;

  // Top-N visible labels (by notional desc); top-3 within longs.
  const labelVisible = new Set(longs.slice(0, LABEL_VISIBLE_COUNT).map((p) => p.symbol));

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

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      {/* Main bar — long composition */}
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
          {longs.length === 0 ? (
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
            longs.map((p) => {
              const widthPct = totalLong > 0 ? (p.notional_usd / totalLong) * 100 : 0;
              return (
                <Segment
                  key={p.symbol}
                  pos={p}
                  widthPct={widthPct}
                  showLabel={labelVisible.has(p.symbol)}
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
            ${totalLong.toLocaleString(undefined, { maximumFractionDigits: 0 })} LONG
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
            HEDGES · ${totalShort.toLocaleString(undefined, { maximumFractionDigits: 0 })}
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

      {/* Stat strip — long-only composition cuts (was LONG/SHORT/NET) */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr",
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
          label="Notional"
          value={`$${totalNotional.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          valueColor="var(--t0)"
          sub={
            longs.length > 0
              ? `largest ${longs[0].symbol_base} ${((longs[0].notional_usd / totalNotional) * 100).toFixed(0)}%`
              : ""
          }
        />
        <Stat
          label="Leverage Ratio"
          value={`${leverageRatio.toFixed(2)}× equity`}
          valueColor={
            leverageRatio < 1.5
              ? "var(--green)"
              : leverageRatio < 3
                ? "var(--amber)"
                : "var(--red)"
          }
          sub={`equity $${equity.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
        />
      </div>

      {/* Single-column position breakdown */}
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
            ${totalLong.toLocaleString(undefined, { maximumFractionDigits: 0 })} TOTAL
          </span>
        </div>
        {longs.length === 0 ? (
          <div style={{ fontSize: 10, color: "var(--t3)" }}>none</div>
        ) : (
          longs.map((p, i) => (
            <div
              key={p.symbol}
              style={{
                display: "grid",
                gridTemplateColumns: "100px 1fr 100px 80px",
                fontSize: 11,
                padding: "4px 0",
                borderBottom: i === longs.length - 1 ? "none" : "1px dashed var(--line)",
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

function Segment({
  pos, widthPct, showLabel,
}: {
  pos: LivePosition;
  widthPct: number;
  showLabel: boolean;
}) {
  return (
    <div
      title={`${pos.symbol_base} · $${pos.notional_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}${
        pos.leverage ? ` · ${pos.leverage.toFixed(1)}×` : ""
      }`}
      style={{
        height: "100%",
        width: `${widthPct}%`,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        color: "var(--green)",
        background: "rgba(0, 200, 150, 0.32)",
        borderRight: "1px solid rgba(8, 8, 9, 0.4)",
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

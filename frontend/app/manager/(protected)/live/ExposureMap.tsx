"use client";

/**
 * frontend/app/manager/(protected)/live/ExposureMap.tsx
 * ========================================================
 * Exposure · Long vs Short · LIVE (Data Dictionary §7).
 *
 * Single horizontal divergent bar centered on zero. Right side =
 * longs, left side = shorts. Sub-segments per position, ordered by
 * notional desc within each side.
 *
 * Bar scale: per-side max-anchored. The larger side fills 50% of the
 * bar; the smaller side scales by ratio. Each sub-segment width =
 * `notional_i / max_side × 50%` of total bar.
 *
 * Below the bar:
 *   * Three-stat strip: LONG / SHORT / NET totals + position counts.
 *   * Two-column position breakdown grouped by side.
 */

import type { LivePosition, AccountSnapshot } from "./types";

interface Props {
  positions: LivePosition[];
  account: AccountSnapshot | null;
}

export default function ExposureMap({ positions, account }: Props) {
  const longs = positions.filter((p) => p.side === "long");
  const shorts = positions.filter((p) => p.side === "short");
  longs.sort((a, b) => b.notional_usd - a.notional_usd);
  shorts.sort((a, b) => b.notional_usd - a.notional_usd);

  const longTotal = longs.reduce((acc, p) => acc + p.notional_usd, 0);
  const shortTotal = shorts.reduce((acc, p) => acc + p.notional_usd, 0);
  const totalNotional = longTotal + shortTotal;
  const net = longTotal - shortTotal;
  const equity = account?.total_equity_usd ?? 0;
  const netMultEquity = equity > 0 ? Math.abs(net) / equity : 0;

  const maxSide = Math.max(longTotal, shortTotal);

  // Each segment width = notional_i / maxSide × 50% of total bar.
  const longSegments = longs.map((p) => ({
    pos: p,
    widthPct: maxSide > 0 ? (p.notional_usd / maxSide) * 50 : 0,
  }));
  const shortSegments = shorts.map((p) => ({
    pos: p,
    widthPct: maxSide > 0 ? (p.notional_usd / maxSide) * 50 : 0,
  }));

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
      {/* Header meta */}
      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          fontSize: 10,
          color: "var(--t3)",
          letterSpacing: "0.06em",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          fontVariantNumeric: "tabular-nums",
        }}
      >
        NET{" "}
        <span
          style={{
            fontWeight: 700,
            color: net > 0 ? "var(--green)" : net < 0 ? "var(--red)" : "var(--t1)",
            marginLeft: 4,
          }}
        >
          {net > 0 ? "+" : net < 0 ? "−" : ""}${Math.abs(net).toLocaleString(undefined, { maximumFractionDigits: 0 })}
          {" "}
          {net > 0 ? "LONG" : net < 0 ? "SHORT" : "FLAT"}
        </span>
        <span style={{ marginLeft: 8 }}>· {netMultEquity.toFixed(2)}× equity</span>
      </div>

      {/* Bar */}
      <div style={{ position: "relative", paddingTop: 18 }}>
        <div
          style={{
            position: "absolute",
            left: "50%",
            top: 0,
            transform: "translateX(-50%)",
            fontSize: 9,
            color: "var(--t3)",
            letterSpacing: "0.1em",
          }}
        >
          0
        </div>
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
          <div
            style={{
              flex: 1,
              display: "flex",
              justifyContent: "flex-end",
              height: "100%",
            }}
          >
            {shortSegments.map(({ pos, widthPct }) => (
              <Segment key={pos.symbol} pos={pos} widthPct={widthPct} side="short" />
            ))}
          </div>
          {/* Center line */}
          <div
            style={{
              position: "absolute",
              left: "50%",
              top: -4,
              bottom: -4,
              width: 1,
              background: "var(--t1)",
              zIndex: 2,
            }}
          />
          <div
            style={{
              flex: 1,
              display: "flex",
              justifyContent: "flex-start",
              height: "100%",
            }}
          >
            {longSegments.map(({ pos, widthPct }) => (
              <Segment key={pos.symbol} pos={pos} widthPct={widthPct} side="long" />
            ))}
          </div>
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
          <span style={{ color: "var(--red)" }}>SHORT ${shortTotal.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
          <span />
          <span style={{ color: "var(--green)" }}>LONG ${longTotal.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
        </div>
      </div>

      {/* Stat strip */}
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
          label="LONG"
          value={`$${longTotal.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          valueColor="var(--green)"
          sub={
            totalNotional > 0
              ? `${((longTotal / totalNotional) * 100).toFixed(1)}% · ${longs.length} position${longs.length === 1 ? "" : "s"}`
              : ""
          }
        />
        <Stat
          label="SHORT"
          value={`$${shortTotal.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          valueColor="var(--red)"
          sub={
            totalNotional > 0
              ? `${((shortTotal / totalNotional) * 100).toFixed(1)}% · ${shorts.length} position${shorts.length === 1 ? "" : "s"}`
              : ""
          }
        />
        <Stat
          label="NET"
          value={`${net > 0 ? "+" : net < 0 ? "−" : ""}$${Math.abs(net).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          valueColor={net > 0 ? "var(--green)" : net < 0 ? "var(--red)" : "var(--t1)"}
          sub={
            net > 0
              ? "unhedged crypto beta"
              : net < 0
                ? "net short"
                : "balanced"
          }
        />
      </div>

      {/* Two-column position breakdown */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 1,
          background: "var(--line)",
          border: "1px solid var(--line)",
          borderRadius: 3,
          overflow: "hidden",
        }}
      >
        <Column
          headLabel="LONGS"
          headTotal={`$${longTotal.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          headColor="var(--green)"
          positions={longs}
        />
        <Column
          headLabel="SHORTS"
          headTotal={`$${shortTotal.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          headColor="var(--red)"
          positions={shorts}
        />
      </div>
    </div>
  );
}

function Segment({
  pos, widthPct, side,
}: {
  pos: LivePosition;
  widthPct: number;
  side: "long" | "short";
}) {
  return (
    <div
      title={`${pos.symbol_base} · $${pos.notional_usd.toFixed(0)}`}
      style={{
        height: "100%",
        width: `${widthPct}%`,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        color: side === "long" ? "var(--green)" : "var(--red)",
        background:
          side === "long"
            ? "rgba(0, 200, 150, 0.32)"
            : "rgba(255, 77, 77, 0.32)",
        borderRight: "1px solid rgba(8, 8, 9, 0.4)",
        overflow: "hidden",
        whiteSpace: "nowrap",
        transition: "width 280ms ease",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {pos.symbol_base}
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

function Column({
  headLabel, headTotal, headColor, positions,
}: {
  headLabel: string;
  headTotal: string;
  headColor: string;
  positions: LivePosition[];
}) {
  return (
    <div style={{ background: "var(--bg2)", padding: "10px 12px" }}>
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
        <span>{headLabel}</span>
        <span style={{ color: headColor, fontWeight: 700 }}>{headTotal}</span>
      </div>
      {positions.length === 0 ? (
        <div style={{ fontSize: 10, color: "var(--t3)" }}>none</div>
      ) : (
        positions.map((p) => (
          <div
            key={p.symbol}
            style={{
              display: "flex",
              justifyContent: "space-between",
              fontSize: 11,
              padding: "4px 0",
              borderBottom: "1px dashed var(--line)",
              fontVariantNumeric: "tabular-nums",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            <span style={{ color: "var(--t0)", fontWeight: 700 }}>{p.symbol_base}</span>
            <span style={{ color: "var(--t3)" }}>
              ${p.notional_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}
              {" · "}
              {p.leverage ? `${p.leverage.toFixed(1)}×` : "—"}
            </span>
          </div>
        ))
      )}
    </div>
  );
}

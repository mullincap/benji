"use client";

/**
 * frontend/app/manager/(protected)/live/Waterfall.tsx
 * ======================================================
 * PnL Attribution · Today (Data Dictionary §6).
 *
 * Horizontal divergent bars, one per position, sorted by absolute today
 * contribution descending. Bar widths scale to the largest absolute
 * contribution = 50% of bar zone. Right of each bar shows the signed
 * dollar value.
 *
 * "Anchor missing" badge: when a position's today_pnl_usd is null
 * (no anchor row in position_snapshots for today), the bar is replaced
 * with a thin neutral track and a "ANCHOR MISSING" badge so the user
 * isn't shown a fake zero.
 */

import type { LivePosition } from "./types";

interface Props {
  positions: LivePosition[];
  /** Session-window label e.g. "session 06:35–close" or "today UTC".
   *  v1 always uses UTC day per Data Dictionary §19 decision 5. */
  sessionLabel?: string;
}

export default function Waterfall({ positions, sessionLabel = "today UTC" }: Props) {
  // Sort by absolute today contribution desc; positions with missing
  // anchors fall to the bottom.
  const sorted = [...positions].sort((a, b) => {
    const av = a.today_pnl_usd === null ? -1 : Math.abs(a.today_pnl_usd);
    const bv = b.today_pnl_usd === null ? -1 : Math.abs(b.today_pnl_usd);
    return bv - av;
  });

  const knownContribs = sorted
    .map((p) => p.today_pnl_usd)
    .filter((v): v is number => v !== null);
  const maxAbs = knownContribs.length > 0
    ? Math.max(...knownContribs.map((v) => Math.abs(v)))
    : 0;
  const netToday = knownContribs.reduce((acc, v) => acc + v, 0);
  const allAnchorsMissing = knownContribs.length === 0 && sorted.length > 0;

  // Round-up axis tick to nearest $10 above max abs contribution; min $10.
  const axisMax = Math.max(10, Math.ceil(maxAbs / 10) * 10);

  if (sorted.length === 0) {
    return (
      <EmptyMsg label="No PnL activity in window" />
    );
  }

  if (allAnchorsMissing) {
    return (
      <EmptyMsg
        label="Anchors missing for today — waterfall populates after the next 00:00 UTC anchor run"
      />
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      {/* Header meta: net + session label */}
      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          gap: 12,
          fontSize: 10,
          color: "var(--t3)",
          letterSpacing: "0.06em",
          marginBottom: 4,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          fontVariantNumeric: "tabular-nums",
        }}
      >
        <span>
          NET{" "}
          <span
            style={{
              fontWeight: 700,
              color: netToday >= 0 ? "var(--green)" : "var(--red)",
            }}
          >
            {netToday >= 0 ? "+" : "−"}${Math.abs(netToday).toFixed(2)}
          </span>
        </span>
        <span>· {sessionLabel}</span>
      </div>

      {sorted.map((p) => (
        <Row key={p.symbol} position={p} maxAbs={maxAbs} />
      ))}

      {/* Axis */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "60px 1fr 80px",
          gap: 10,
          marginTop: 6,
          fontSize: 9,
          color: "var(--t3)",
          letterSpacing: "0.1em",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          fontVariantNumeric: "tabular-nums",
        }}
      >
        <div />
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            borderTop: "1px dashed var(--line)",
            paddingTop: 4,
          }}
        >
          <span>−${axisMax}</span>
          <span>0</span>
          <span>+${axisMax}</span>
        </div>
        <div />
      </div>
    </div>
  );
}

function Row({ position, maxAbs }: { position: LivePosition; maxAbs: number }) {
  const v = position.today_pnl_usd;
  const isMissing = v === null;
  const widthPct =
    !isMissing && maxAbs > 0 ? (Math.abs(v) / maxAbs) * 50 : 0;

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "60px 1fr 80px",
        alignItems: "center",
        gap: 10,
        height: 26,
        transition: "background 200ms ease",
      }}
    >
      <div
        style={{
          fontSize: 11,
          fontWeight: 700,
          textAlign: "right",
          letterSpacing: "0.04em",
          color: "var(--t1)",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        {position.symbol_base}
      </div>

      {/* Bar zone with center line */}
      <div
        style={{
          position: "relative",
          height: "100%",
          background:
            "linear-gradient(to right, transparent calc(50% - 0.5px), var(--line2) calc(50% - 0.5px), var(--line2) calc(50% + 0.5px), transparent calc(50% + 0.5px))",
        }}
      >
        {!isMissing && v !== 0 && (
          <div
            style={{
              position: "absolute",
              top: 0,
              bottom: 0,
              width: `${widthPct}%`,
              borderRadius: 2,
              transition: "width 280ms ease",
              ...(v > 0
                ? {
                    background: "rgba(0, 200, 150, 0.30)",
                    borderLeft: "2px solid var(--green)",
                    left: "50%",
                  }
                : {
                    background: "rgba(255, 77, 77, 0.30)",
                    borderRight: "2px solid var(--red)",
                    right: "50%",
                  }),
            }}
          />
        )}
        {isMissing && (
          <div
            style={{
              position: "absolute",
              top: "50%",
              left: "50%",
              transform: "translate(-50%, -50%)",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.1em",
              padding: "2px 6px",
              border: "1px solid var(--amber)",
              borderRadius: 2,
              color: "var(--amber)",
              background: "var(--amber-dim)",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            ANCHOR MISSING
          </div>
        )}
      </div>

      {/* Value */}
      <div
        style={{
          fontSize: 11,
          fontWeight: 700,
          textAlign: "left",
          color: isMissing
            ? "var(--t3)"
            : v > 0
              ? "var(--green)"
              : v < 0
                ? "var(--red)"
                : "var(--t2)",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {isMissing
          ? "—"
          : `${v > 0 ? "+" : v < 0 ? "−" : ""}$${Math.abs(v).toFixed(2)}`}
      </div>
    </div>
  );
}

function EmptyMsg({ label }: { label: string }) {
  return (
    <div
      style={{
        height: 200,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        textAlign: "center",
        padding: "0 24px",
        color: "var(--t3)",
        fontSize: 11,
        letterSpacing: "0.06em",
        lineHeight: 1.55,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {label}
    </div>
  );
}

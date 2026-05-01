"use client";

/**
 * frontend/app/manager/(protected)/live/EffectiveNGauge.tsx
 * ============================================================
 * Effective-N gauge (Data Dictionary §9a). Lives at the top of the
 * Factor Decomposition card.
 *
 * Headline number = effective-N (correlation-aware diversification
 * ratio: see correlation_cache.py for the formula). Detail prose and
 * headline color share absolute thresholds:
 *   < 2  : red    "concentrated, not diversified"
 *   2-4  : amber  "moderately concentrated"
 *   > 4  : green  "well diversified"
 *
 * Track band split is proportional: 0→2 red, 2→4 amber, 4→nominal_count
 * green. With ≤4 positions the green band collapses to zero — fully-
 * diversified is unreachable for a book that small, which is honest.
 * Marker sits at (effective_N / nominal_count) × 100% on the track.
 */

import type { CoverageMatrixResponse } from "./types";

interface Props {
  data: CoverageMatrixResponse | null;
}

export default function EffectiveNGauge({ data }: Props) {
  if (!data) {
    return <Skeleton />;
  }
  const total = data.nominal_count;
  const value = data.effective_n;

  // Empty / insufficient cases
  if (total === 0) {
    return <Empty msg="No open positions" />;
  }
  if (value === null) {
    return (
      <Empty msg="Need ≥ 14 days of kline history per position to compute effective-N" />
    );
  }

  // Absolute zone thresholds matching the spec: 0→2 red, 2→4 amber,
  // 4→total green. Color and prose use the same boundaries.
  const RED_HI = 2;
  const AMBER_HI = 4;

  const color =
    value < RED_HI
      ? "var(--red)"
      : value < AMBER_HI
        ? "var(--amber)"
        : "var(--green)";

  const detail =
    value < RED_HI
      ? `Your ${total} nominal position${total === 1 ? "" : "s"} behave like ${value.toFixed(1)} independent. The book is concentrated, not diversified — most positions carry the same underlying exposure.`
      : value < AMBER_HI
        ? `Your ${total} nominal positions behave like ${value.toFixed(1)} independent. Moderately concentrated — some shared exposure across the book.`
        : `Your ${total} nominal positions behave like ${value.toFixed(1)} independent. The book is well-diversified — most positions are carrying distinct exposure.`;

  // Marker position in [0, 100]
  const markerPct = Math.max(0, Math.min(100, (value / total) * 100));

  const benefit = data.diversification_benefit_pct;

  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: "14px 16px 12px",
      }}
    >
      <div
        style={{
          display: "flex",
          gap: 16,
          alignItems: "flex-start",
          marginBottom: 14,
        }}
      >
        <div
          style={{
            flexShrink: 0,
            paddingRight: 16,
            borderRight: "1px solid var(--line)",
          }}
        >
          <div
            style={{
              fontSize: 38,
              fontWeight: 700,
              color,
              lineHeight: 1,
              fontVariantNumeric: "tabular-nums",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            {value.toFixed(1)}
          </div>
          <div
            style={{
              fontSize: 10,
              color: "var(--t3)",
              letterSpacing: "0.1em",
              marginTop: 4,
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            / {total} NOMINAL
          </div>
        </div>
        <div
          style={{
            fontSize: 11,
            color: "var(--t1)",
            lineHeight: 1.55,
            flex: 1,
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          {detail}
          {benefit !== null && (
            <>
              {" "}
              <span style={{ color: "var(--t2)" }}>
                Diversification benefit:{" "}
                <span
                  style={{
                    color:
                      benefit < 5 ? "var(--red)" : benefit < 20 ? "var(--amber)" : "var(--green)",
                    fontWeight: 700,
                  }}
                >
                  {benefit.toFixed(1)}%
                </span>{" "}
                vs holding as independent silos.
              </span>
            </>
          )}
        </div>
      </div>

      {/* Track — band widths proportional to absolute thresholds.
          Green band collapses to zero when total ≤ 4 (fully diversified
          is unreachable for a small book; surface that honestly). */}
      <div
        style={{
          position: "relative",
          height: 10,
          display: "flex",
          borderRadius: 2,
          overflow: "hidden",
          border: "1px solid var(--line)",
        }}
      >
        <div
          style={{
            flex: Math.min(RED_HI, total),
            background: "rgba(255, 77, 77, 0.45)",
          }}
        />
        <div
          style={{
            flex: Math.max(0, Math.min(AMBER_HI, total) - RED_HI),
            background: "rgba(240, 165, 0, 0.40)",
          }}
        />
        <div
          style={{
            flex: Math.max(0, total - AMBER_HI),
            background: "rgba(0, 200, 150, 0.40)",
          }}
        />
        <div
          style={{
            position: "absolute",
            top: -3,
            bottom: -3,
            width: 2,
            left: `${markerPct}%`,
            background: "var(--t0)",
            boxShadow: "0 0 0 1px rgba(0,0,0,0.6)",
          }}
        />
      </div>
      <div
        style={{
          display: "flex",
          fontSize: 9,
          color: "var(--t3)",
          letterSpacing: "0.1em",
          marginTop: 6,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        <span style={{ flex: Math.min(RED_HI, total), color: "var(--red)" }}>
          0 · CONCENTRATED
        </span>
        {total > RED_HI && (
          <span
            style={{
              flex: Math.max(0, Math.min(AMBER_HI, total) - RED_HI),
              color: "var(--amber)",
              textAlign: total > AMBER_HI ? "left" : "right",
              paddingLeft: 4,
            }}
          >
            {RED_HI} · MIXED
          </span>
        )}
        {total > AMBER_HI && (
          <span
            style={{
              flex: Math.max(0, total - AMBER_HI),
              color: "var(--green)",
              paddingLeft: 4,
            }}
          >
            {AMBER_HI} · DIVERSIFIED
          </span>
        )}
        <span style={{ color: "var(--t3)", marginLeft: "auto", paddingLeft: 4 }}>
          {total}
        </span>
      </div>
    </div>
  );
}

function Skeleton() {
  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: "14px 16px",
        height: 110,
      }}
    />
  );
}

function Empty({ msg }: { msg: string }) {
  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: "14px 16px",
        color: "var(--t3)",
        fontSize: 11,
        letterSpacing: "0.06em",
        textAlign: "center",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {msg}
    </div>
  );
}

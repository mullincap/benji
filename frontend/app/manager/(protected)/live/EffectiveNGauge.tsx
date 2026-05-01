"use client";

/**
 * frontend/app/manager/(protected)/live/EffectiveNGauge.tsx
 * ============================================================
 * Effective-N gauge (Data Dictionary §9a). Lives at the top of the
 * Factor Decomposition card; the factor-decomposition bar below it
 * lands in step 11.
 *
 * Headline number = effective-N. Detail prose by zone:
 *   < 2  : "concentrated, not diversified"
 *   2-4  : "moderately concentrated"
 *   4+   : "well diversified"
 *
 * Headline color follows zone: red < 2, amber 2-4, green > 4.
 * Track has three equal bands (red / amber / green) with a marker
 * positioned at value/total × 100% (clamped to 0-100). Total = the
 * nominal_count (number of open positions) per the spec — gauge maxes
 * out at "fully diversified" which means independent positions equal
 * to the nominal count.
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

  // Zone bands are configured against `total`. Equal thirds:
  // red 0 to total/3, amber total/3 to 2·total/3, green 2·total/3 to total.
  const lo = total / 3;
  const hi = (2 * total) / 3;

  const color =
    value < lo
      ? "var(--red)"
      : value < hi
        ? "var(--amber)"
        : "var(--green)";

  const detail =
    value < 2
      ? `Your ${total} nominal position${total === 1 ? "" : "s"} behave like ${value.toFixed(1)} independent. The book is concentrated, not diversified — most positions carry the same underlying exposure.`
      : value < 4
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
            / {total.toFixed(1)} EFFECTIVE
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

      {/* Track */}
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
        <div style={{ flex: 1, background: "rgba(255, 77, 77, 0.45)" }} />
        <div style={{ flex: 1, background: "rgba(240, 165, 0, 0.40)" }} />
        <div style={{ flex: 1, background: "rgba(0, 200, 150, 0.40)" }} />
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
          justifyContent: "space-between",
          fontSize: 9,
          color: "var(--t3)",
          letterSpacing: "0.1em",
          marginTop: 6,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        <span>0</span>
        <span style={{ color: "var(--red)" }}>{lo.toFixed(1)} · ALARM</span>
        <span style={{ color: "var(--amber)" }}>{hi.toFixed(1)} · HEALTHY</span>
        <span style={{ color: "var(--green)" }}>{total.toFixed(1)} · FULLY DIVERSE</span>
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

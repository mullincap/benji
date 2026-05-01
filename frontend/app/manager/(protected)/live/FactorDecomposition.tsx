"use client";

/**
 * frontend/app/manager/(protected)/live/FactorDecomposition.tsx
 * ===============================================================
 * Factor Decomposition · 30D rolling (Data Dictionary §9b).
 *
 * Three-segment stacked horizontal bar:
 *   BTC   (amber)  — variance explained by BTC factor
 *   ALT   (violet) — variance explained by equal-weighted alt index
 *   IDIO  (green)  — residual / idiosyncratic
 *
 * Three rows below the bar break out portfolio-level dollar β's plus
 * the variance shares numerically. The callout text below the rows is
 * rule-based:
 *   * BTC  > 60%  → "Your book moves with BTC. Diversification across
 *                   alts won't reduce risk that comes from BTC moves."
 *   * ALT  > 50%  → "Your book is broad-alt directional — moves with
 *                   the alt complex more than BTC specifically."
 *   * IDIO > 50%  → "Names matter more than market direction here.
 *                   Drawdowns will be position-specific."
 *   * otherwise   → "Mixed factor exposure — no single driver dominates."
 */

import type { FactorDecompositionResponse } from "./types";

const COLOR_BTC = "var(--amber)";
const COLOR_ALT = "#8a6bff";  // violet, matches the §9b mockup palette
const COLOR_IDIO = "var(--green)";

interface Props {
  data: FactorDecompositionResponse | null;
}

export default function FactorDecomposition({ data }: Props) {
  if (!data) {
    return <Skeleton />;
  }
  if (!data.portfolio) {
    return (
      <Empty msg="Need ≥ 14 days of kline history per position to decompose factors" />
    );
  }
  const pf = data.portfolio;
  const btcPct = pf.var_btc_pct;
  const altPct = pf.var_alt_pct;
  const idioPct = pf.var_idio_pct;
  const callout = ruleBasedCallout(btcPct, altPct, idioPct);

  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: "14px 16px 12px",
        display: "flex",
        flexDirection: "column",
        gap: 12,
      }}
    >
      <Header
        memberCount={data.alt_index_member_count}
        targetCount={data.alt_index_target_count}
        nDays={data.n_days}
      />

      <StackedBar btcPct={btcPct} altPct={altPct} idioPct={idioPct} />

      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        <FactorRow
          label="BTC factor"
          color={COLOR_BTC}
          pct={btcPct}
          beta={pf.beta_btc}
        />
        <FactorRow
          label="ALT factor"
          color={COLOR_ALT}
          pct={altPct}
          beta={pf.beta_alt}
        />
        <FactorRow
          label="Idiosyncratic"
          color={COLOR_IDIO}
          pct={idioPct}
          beta={null}
        />
      </div>

      <Callout text={callout} />
    </div>
  );
}

function Header({
  memberCount, targetCount, nDays,
}: {
  memberCount: number;
  targetCount: number;
  nDays: number;
}) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "baseline",
        fontSize: 9,
        letterSpacing: "0.1em",
        color: "var(--t3)",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      <span style={{ color: "var(--t2)" }}>
        VARIANCE ATTRIBUTION · {nDays}D ROLLING
      </span>
      <span title="Number of alt-index members with usable history (out of configured pool)">
        ALT INDEX · {memberCount} / {targetCount}
      </span>
    </div>
  );
}

function StackedBar({
  btcPct, altPct, idioPct,
}: { btcPct: number; altPct: number; idioPct: number }) {
  // Floor at 0% to handle any negative ridge artifact safely; clamp
  // total to 100 to keep the bar from overflowing on a stale-cache
  // edge case where the three drift slightly past 100.
  const b = Math.max(0, btcPct);
  const a = Math.max(0, altPct);
  const i = Math.max(0, idioPct);
  const total = b + a + i;
  const norm = total > 0 ? 100 / total : 0;

  return (
    <div
      style={{
        position: "relative",
        height: 28,
        display: "flex",
        borderRadius: 2,
        overflow: "hidden",
        border: "1px solid var(--line)",
      }}
    >
      <Segment color={COLOR_BTC} pct={b * norm} label="BTC" />
      <Segment color={COLOR_ALT} pct={a * norm} label="ALT" />
      <Segment color={COLOR_IDIO} pct={i * norm} label="IDIO" />
    </div>
  );
}

function Segment({
  color, pct, label,
}: { color: string; pct: number; label: string }) {
  if (pct <= 0) return null;
  // Hide the in-bar label below ~6% so it doesn't clip / overlap.
  const showLabel = pct >= 6;
  return (
    <div
      style={{
        background: color,
        flex: pct,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        letterSpacing: "0.08em",
        color: "rgba(0, 0, 0, 0.78)",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        minWidth: 0,
      }}
    >
      {showLabel ? `${label} ${pct.toFixed(0)}%` : ""}
    </div>
  );
}

function FactorRow({
  label, color, pct, beta,
}: {
  label: string;
  color: string;
  pct: number;
  beta: number | null;
}) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "10px 1fr auto auto",
        alignItems: "center",
        gap: 10,
        fontSize: 11,
        color: "var(--t1)",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      <span
        style={{
          width: 10,
          height: 10,
          borderRadius: 2,
          background: color,
        }}
      />
      <span style={{ color: "var(--t1)" }}>{label}</span>
      <span
        style={{
          color: "var(--t2)",
          fontVariantNumeric: "tabular-nums",
          minWidth: 90,
          textAlign: "right",
        }}
        title={beta === null ? "" : "$ portfolio PnL per +100% factor return"}
      >
        {beta === null ? "—" : `β = ${formatDollarBeta(beta)}`}
      </span>
      <span
        style={{
          fontWeight: 700,
          color: "var(--t0)",
          fontVariantNumeric: "tabular-nums",
          minWidth: 50,
          textAlign: "right",
        }}
      >
        {pct.toFixed(1)}%
      </span>
    </div>
  );
}

function Callout({ text }: { text: string }) {
  return (
    <div
      style={{
        fontSize: 11,
        color: "var(--t2)",
        lineHeight: 1.55,
        borderLeft: "2px solid var(--line2)",
        paddingLeft: 10,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {text}
    </div>
  );
}

function ruleBasedCallout(btc: number, alt: number, idio: number): string {
  if (btc > 60) {
    return "Your book moves with BTC. Diversification across alts won't reduce risk that comes from BTC moves themselves.";
  }
  if (alt > 50) {
    return "Your book is broad-alt directional — moves with the alt complex more than BTC specifically.";
  }
  if (idio > 50) {
    return "Names matter more than market direction here. Drawdowns will be position-specific, not market-driven.";
  }
  return "Mixed factor exposure — no single driver dominates.";
}

function formatDollarBeta(beta: number): string {
  // β is portfolio $ PnL per +100% factor return — typically thousands
  // for a $5k–$20k book. Format with sign and commas.
  const sign = beta >= 0 ? "+" : "−";
  const abs = Math.abs(beta);
  if (abs >= 1000) {
    return `${sign}$${(abs / 1000).toFixed(1)}K`;
  }
  return `${sign}$${abs.toFixed(0)}`;
}

function Skeleton() {
  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: "14px 16px",
        height: 180,
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

/**
 * frontend/app/compiler/(protected)/symbols/page.tsx
 * ==================================================
 * Phase 3 placeholder. The real Symbols page is built in Phase 6.
 */

export default function CompilerSymbolsPage() {
  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
        }}>
          Compiler · Symbols
        </div>

        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Symbol Inspector
        </h1>

        <div style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          padding: "20px 24px",
          fontSize: 10,
          color: "var(--t1)",
          lineHeight: 1.6,
        }}>
          <div style={{
            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
            color: "var(--t3)", textTransform: "uppercase", marginBottom: 8,
          }}>
            Phase 6 Placeholder
          </div>
          The Symbol Inspector will render here in Phase 6: a search input, 15
          per-endpoint completeness bars (close / volume / OI / funding / LS / trade_delta /
          long_liqs / short_liqs / last_bid_depth / last_ask_depth / last_depth_imbalance /
          last_spread_pct / spread_pct / bid_ask_imbalance / basis_pct), and a 30-day
          row count sparkline. Data source:{" "}
          <code style={{ color: "var(--t0)" }}>GET /api/compiler/symbols/{"{symbol}"}</code>.
        </div>
      </div>
    </div>
  );
}

/**
 * frontend/app/indexer/(protected)/strategies/page.tsx
 * ====================================================
 * Phase 2 placeholder. Phase 6 will replace this with the real Strategies
 * page: list of audit.strategies with strategy_versions nested under each,
 * showing is_active, published_at, and a config JSONB excerpt. Read-only —
 * no edit affordances this round. Backed by GET /api/indexer/strategies.
 */

export default function IndexerStrategiesPage() {
  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 8,
        }}>
          Indexer · Strategies
        </div>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Strategies
        </h1>
        <div style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          padding: "16px 18px",
        }}>
          <div style={{
            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
            color: "var(--t3)", textTransform: "uppercase",
            marginBottom: 8,
          }}>
            Phase 6 Placeholder
          </div>
          <div style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.6 }}>
            This page will render the <code>audit.strategies</code> registry
            with versions nested under each strategy, showing is_active,
            published_at, and a config JSONB excerpt. Read-only this round —
            no edit affordances. Backed by{" "}
            <code>GET /api/indexer/strategies</code>.
          </div>
        </div>
      </div>
    </div>
  );
}

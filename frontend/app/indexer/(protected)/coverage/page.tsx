/**
 * frontend/app/indexer/(protected)/coverage/page.tsx
 * ==================================================
 * Phase 2 placeholder. Phase 3 will replace this with the real Coverage page:
 * per-metric KPI cards, three stacked heatmaps (price / open_interest /
 * volume), and a gap table grouped by metric — driven by
 * GET /api/indexer/coverage?days=N.
 */

export default function IndexerCoveragePage() {
  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 8,
        }}>
          Indexer · Coverage
        </div>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Coverage Map
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
            Phase 3 Placeholder
          </div>
          <div style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.6 }}>
            This page will render leaderboard completeness per metric per day,
            measured against the strict 1440 × 333 expected row count. KPI
            cards, three stacked heatmaps (price · open_interest · volume), and
            a gap table grouped by metric. Backed by{" "}
            <code>GET /api/indexer/coverage?days=N</code>.
          </div>
        </div>
      </div>
    </div>
  );
}

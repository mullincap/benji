/**
 * frontend/app/indexer/(protected)/signals/page.tsx
 * =================================================
 * Phase 2 placeholder. Phase 5 will replace this with the real Signals page:
 * source filter chips (live / backtest / research / all), date column,
 * strategy version label, sit_flat badge, filter_name, and a click-to-detail
 * row showing the signal items. Backed by GET /api/indexer/signals and
 * GET /api/indexer/signals/{signal_batch_id}.
 */

export default function IndexerSignalsPage() {
  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 8,
        }}>
          Indexer · Signals
        </div>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Daily Signals
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
            Phase 5 Placeholder
          </div>
          <div style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.6 }}>
            This page will render the recent{" "}
            <code>user_mgmt.daily_signals</code> history with source filter
            chips (live · backtest · research), strategy version labels, the
            sit_flat badge, and a click-to-expand symbol list per batch. Backed
            by <code>GET /api/indexer/signals</code>.
          </div>
        </div>
      </div>
    </div>
  );
}

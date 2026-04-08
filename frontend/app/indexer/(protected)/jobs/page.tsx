/**
 * frontend/app/indexer/(protected)/jobs/page.tsx
 * ==============================================
 * Phase 2 placeholder. Phase 4 will replace this with the real Jobs page:
 * job table with job_type filter chips (leaderboard / overlap / full / all),
 * status badges, polling, and live duration column — driven by
 * GET /api/indexer/jobs and GET /api/indexer/jobs/{job_id}. The page will
 * include an honest empty state for the current 0-row reality.
 */

export default function IndexerJobsPage() {
  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 8,
        }}>
          Indexer · Jobs
        </div>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Indexer Jobs
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
            Phase 4 Placeholder
          </div>
          <div style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.6 }}>
            This page will render the recent <code>market.indexer_jobs</code>{" "}
            history with job_type filter chips, status badges, 10s polling
            while jobs are running, and a live duration column. Backed by{" "}
            <code>GET /api/indexer/jobs</code>.
          </div>
        </div>
      </div>
    </div>
  );
}

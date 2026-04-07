/**
 * frontend/app/compiler/(protected)/jobs/page.tsx
 * ===============================================
 * Phase 3 placeholder. The real Jobs page is built in Phase 5.
 */

export default function CompilerJobsPage() {
  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
        }}>
          Compiler · Jobs
        </div>

        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Job Runs
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
            Phase 5 Placeholder
          </div>
          The Jobs page will render here in Phase 5: a sortable table of recent
          compiler runs with status badges (COMPLETE / RUNNING / FAILED / STALE),
          progress bars for in-flight jobs, and 10s polling. Data source:{" "}
          <code style={{ color: "var(--t0)" }}>GET /api/compiler/jobs</code>.
        </div>
      </div>
    </div>
  );
}

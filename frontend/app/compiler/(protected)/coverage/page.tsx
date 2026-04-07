/**
 * frontend/app/compiler/(protected)/coverage/page.tsx
 * ===================================================
 * Phase 3 placeholder. The real Coverage page is built in Phase 4 against
 * GET /api/compiler/coverage and GET /api/compiler/gaps.
 *
 * Renders just enough chrome (24px page title + section label) to verify
 * the protected layout, sidebar active state, and Topbar amber accent are
 * all wired up correctly.
 */

export default function CompilerCoveragePage() {
  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
        }}>
          Compiler · Coverage
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
          padding: "20px 24px",
          fontSize: 10,
          color: "var(--t1)",
          lineHeight: 1.6,
        }}>
          <div style={{
            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
            color: "var(--t3)", textTransform: "uppercase", marginBottom: 8,
          }}>
            Phase 4 Placeholder
          </div>
          The Coverage page will render here in Phase 4: 4 KPI cards (Total Symbols /
          Days Complete / Days With Gaps / Days Missing), a calendar heatmap of the
          last 90 days, and a gap table. Data sources: <code style={{ color: "var(--t0)" }}>GET /api/compiler/coverage</code>{" "}
          and <code style={{ color: "var(--t0)" }}>GET /api/compiler/gaps</code>.
        </div>
      </div>
    </div>
  );
}

"use client";

/**
 * Manager → Live tab (placeholder).
 *
 * Reserved route for an upcoming advanced live view. The simpler "what's
 * open right now" view lives at /manager/positions.
 */

export default function LivePage() {
  return (
    <div
      style={{
        padding: 20,
        height: "100%",
        display: "flex",
        flexDirection: "column",
        gap: 10,
      }}
    >
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "var(--t3)",
            textTransform: "uppercase",
          }}
        >
          Live
        </span>
        <span style={{ fontSize: 9, color: "var(--t3)" }}>
          Advanced live view — coming soon
        </span>
      </div>

      <div
        style={{
          flex: 1,
          background: "var(--bg1)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: 8,
            textAlign: "center",
          }}
        >
          <span
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--green)",
              textTransform: "uppercase",
            }}
          >
            Coming soon
          </span>
          <span
            style={{
              fontSize: 11,
              color: "var(--t2)",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              maxWidth: 360,
            }}
          >
            A richer real-time account view is in the works.
          </span>
          <span style={{ fontSize: 10, color: "var(--t3)", marginTop: 4 }}>
            For current open positions, see{" "}
            <a
              href="/manager/positions"
              style={{ color: "var(--t1)", textDecoration: "underline" }}
            >
              Positions
            </a>
            .
          </span>
        </div>
      </div>
    </div>
  );
}

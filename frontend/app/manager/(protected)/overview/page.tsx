"use client";

export default function OverviewPage() {
  return (
    <div style={{ padding: 28 }}>
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          color: "var(--t3)",
          textTransform: "uppercase",
          marginBottom: 8,
        }}
      >
        Manager · Overview
      </div>
      <div
        style={{
          fontSize: 24,
          fontWeight: 700,
          color: "var(--t0)",
          marginBottom: 20,
        }}
      >
        Portfolio Overview
      </div>
      <div
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          padding: 20,
        }}
      >
        <div
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "var(--t3)",
            textTransform: "uppercase",
            marginBottom: 8,
          }}
        >
          Phase 4 Placeholder
        </div>
        <div style={{ fontSize: 10, color: "var(--t2)", lineHeight: 1.6 }}>
          This page will render 5 KPI cards (TODAY % / WTD % / MTD % / MAX
          DRAWDOWN / TOTAL AUM), equity curve and daily return charts, a
          per-allocation breakdown table, and pipeline status.
        </div>
      </div>
    </div>
  );
}

"use client";

import { useRouter } from "next/navigation";
import { useTrader } from "../context";
import TraderCard from "../components/TraderCard";

export default function TradersPage() {
  const router = useRouter();
  const { instances } = useTrader();

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>

        <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginBottom: 16 }}>
          TRADERS
        </div>

        {instances.length === 0 ? (
          <div style={{
            background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5,
            padding: "40px 24px", textAlign: "center",
          }}>
            <div style={{ fontSize: 10, color: "var(--t2)" }}>
              No active traders &mdash; browse the{" "}
              <span onClick={() => router.push("/trader/strategies")} style={{ color: "var(--green)", cursor: "pointer" }}>Strategies</span>
              {" "}to get started
            </div>
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {instances.map(inst => (
              <TraderCard key={inst.id} inst={inst} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

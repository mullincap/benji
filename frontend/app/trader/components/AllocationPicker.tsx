"use client";

import { useState, useEffect, useRef } from "react";
import { useTrader } from "../context";

interface AllocationPickerProps {
  value: string;
  onChange: (v: string) => void;
  otherAllocated: number;
}

export default function AllocationPicker({ value, onChange, otherAllocated }: AllocationPickerProps) {
  const { exchanges } = useTrader();
  const totalBalance = exchanges.reduce((s, e) => s + e.balance, 0);

  const [mode, setMode] = useState<"bar" | "custom">("bar");
  const [customFocused, setCustomFocused] = useState(false);
  const [displayValue, setDisplayValue] = useState("");
  const barRef = useRef<HTMLDivElement>(null);
  const [dragging, setDragging] = useState(false);

  const thisAllocation = parseInt(value) || 0;
  const available = totalBalance - otherAllocated;

  // Bar proportions
  const otherPct = totalBalance > 0 ? (otherAllocated / totalBalance) * 100 : 0;
  const thisPct = totalBalance > 0 ? Math.min((thisAllocation / totalBalance) * 100, 100 - otherPct) : 0;

  function handlePointerMove(clientX: number) {
    if (!barRef.current) return;
    const rect = barRef.current.getBoundingClientRect();
    const x = Math.max(0, Math.min(clientX - rect.left, rect.width));
    const pct = x / rect.width;
    const rawValue = Math.round(pct * totalBalance - otherAllocated);
    const clamped = Math.max(0, Math.min(rawValue, available));
    onChange(String(clamped));
  }

  useEffect(() => {
    if (!dragging) return;
    function onMove(e: PointerEvent) { handlePointerMove(e.clientX); }
    function onUp() { setDragging(false); }
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    return () => { window.removeEventListener("pointermove", onMove); window.removeEventListener("pointerup", onUp); };
  });

  const fmtDollar = (n: number) => "$" + n.toLocaleString("en-US");

  return (
    <div style={{ marginBottom: 14 }}>


      {mode === "bar" ? (
        <div>
          {/* Large amount + percentage */}
          <div style={{ textAlign: "center", marginBottom: 10 }}>
            <div style={{ fontSize: 38, fontWeight: 700, color: "var(--t0)", fontFamily: "'Space Mono', monospace" }}>{fmtDollar(thisAllocation)}</div>
            <div style={{ fontSize: 10, color: "var(--t2)" }}>
              {totalBalance > 0 ? ((thisAllocation / totalBalance) * 100).toFixed(1) : "0.0"}% of total balance
            </div>
          </div>

          {/* Split bar */}
          <div
            ref={barRef}
            style={{
              position: "relative", height: 28,
              borderRadius: 6, cursor: "pointer",
              userSelect: "none",
              display: "flex",
            }}
            onPointerDown={e => { setDragging(true); handlePointerMove(e.clientX); }}
          >
            {/* Other traders segment */}
            <div style={{
              height: "100%",
              width: `${otherPct}%`,
              background: "var(--bg4)",
              borderRadius: "6px 0 0 6px",
              flexShrink: 0,
            }} />
            {/* This allocation segment */}
            <div style={{
              height: "100%",
              width: `${thisPct}%`,
              background: "var(--green)",
              flexShrink: 0,
            }} />
            {/* Available segment */}
            <div style={{
              height: "100%",
              flex: 1,
              background: "var(--bg3)",
              borderRadius: "0 6px 6px 0",
            }} />
            {/* Draggable handle */}
            <div style={{
              position: "absolute",
              left: `calc(${otherPct + thisPct}%)`,
              top: "50%", transform: "translate(-50%, -50%)",
              width: 16, height: 16, borderRadius: "50%",
              background: "var(--green)", border: "2px solid var(--bg0)",
              boxShadow: "0 0 0 3px var(--green-dim)",
              cursor: "grab", zIndex: 1,
            }} />
          </div>

          {/* Legend */}
          <div style={{ display: "flex", gap: 16, marginTop: 10, fontSize: 10 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, background: "var(--bg4)", flexShrink: 0 }} />
              <span style={{ color: "var(--t2)" }}>Already Allocated {fmtDollar(otherAllocated)}</span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, background: "var(--green)", flexShrink: 0 }} />
              <span style={{ color: "var(--green)" }}>This trader {fmtDollar(thisAllocation)}</span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, background: "var(--bg3)", border: "1px solid var(--line)", flexShrink: 0 }} />
              <span style={{ color: "var(--t3)" }}>Available {fmtDollar(Math.max(0, available - thisAllocation))}</span>
            </div>
          </div>

          {/* Switch to custom */}
          <div
            onClick={() => setMode("custom")}
            style={{ fontSize: 9, color: "var(--t3)", marginTop: 8, cursor: "pointer", textDecoration: "underline" }}
          >
            Enter custom amount →
          </div>
        </div>
      ) : (
        <div>
          {/* Custom input */}
          <div style={{ position: "relative" }}>
            <span style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", fontSize: 16, fontWeight: 700, color: "var(--t2)", pointerEvents: "none" }}>$</span>
            <input
              type="text"
              autoFocus
              value={customFocused ? displayValue : (thisAllocation > 0 ? thisAllocation.toLocaleString("en-US") : "")}
              onChange={e => {
                const raw = e.target.value.replace(/,/g, "").replace(/[^0-9]/g, "");
                const num = parseInt(raw) || 0;
                if (num > available) return;
                setDisplayValue(raw);
                onChange(raw);
              }}
              onFocus={() => {
                setCustomFocused(true);
                setDisplayValue(value);
              }}
              onBlur={e => {
                setCustomFocused(false);
                e.target.style.border = "0.5px solid var(--line)";
              }}
              style={{
                width: "100%", background: "var(--bg2)",
                border: "0.5px solid var(--line)",
                borderRadius: 3, padding: "10px 12px 10px 28px", color: "var(--t0)",
                fontSize: 16, fontWeight: 700, outline: "none",
              }}
              ref={el => { if (el && customFocused) el.style.border = "0.5px solid var(--green)"; }}
            />
          </div>

          {/* Balance note */}
          <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 6 }}>
            Total: {fmtDollar(totalBalance)} · Already allocated: {fmtDollar(otherAllocated)} · Available: {fmtDollar(available)}
          </div>

          {/* Switch to bar */}
          <div
            onClick={() => setMode("bar")}
            style={{ fontSize: 9, color: "var(--t3)", marginTop: 8, cursor: "pointer", textDecoration: "underline" }}
          >
            {"\u2190"} Use split bar
          </div>
        </div>
      )}
    </div>
  );
}

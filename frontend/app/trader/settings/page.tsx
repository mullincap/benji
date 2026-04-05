"use client";

import { useState, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import { useTrader, Exchange, mask } from "../context";

// ─── Step bar ────────────────────────────────────────────────────────────────

function StepBar({ current }: { current: number }) {
  const steps = ["Keys", "Verify", "Permissions", "Done"];
  return (
    <div style={{ display: "flex", alignItems: "center", marginBottom: 20 }}>
      {steps.map((label, i) => {
        const n = i + 1;
        const done = current > n;
        const active = current === n;
        return (
          <div key={n} style={{ display: "flex", alignItems: "center", flex: 1 }}>
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", flex: 1 }}>
              <div style={{
                width: 22, height: 22, borderRadius: "50%",
                background: done ? "var(--green)" : active ? "var(--bg3)" : "var(--bg2)",
                border: done ? "none" : active ? "2px solid var(--green)" : "2px solid var(--line)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 9, fontWeight: 700,
                color: done ? "var(--bg0)" : active ? "var(--green)" : "var(--t2)",
              }}>{done ? "\u2713" : n}</div>
              <span style={{
                marginTop: 4, fontSize: 9, fontWeight: 700,
                color: done ? "var(--green)" : active ? "var(--t0)" : "var(--t2)",
                letterSpacing: "0.12em", textTransform: "uppercase",
              }}>{label}</span>
            </div>
            {i < steps.length - 1 && (
              <div style={{ height: 1, flex: 1, marginBottom: 16, background: done ? "var(--green)" : "var(--line)" }} />
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─── Link wizard ─────────────────────────────────────────────────────────────

function LinkWizard({ onComplete, onCancel }: { onComplete: (e: Exchange) => void; onCancel: () => void }) {
  const [step, setStep] = useState(1);
  const [exName, setExName] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [verifyStatus, setVerifyStatus] = useState<"idle" | "checking" | "ok">("idle");
  const [log, setLog] = useState<string[]>([]);
  const logRef = useRef<HTMLDivElement>(null);

  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [log]);

  function runVerify() {
    setVerifyStatus("checking"); setLog([]);
    const lines = [
      `> Connecting to ${exName || "exchange"} API...`,
      "> Sending signed test request...", "> Response: 200 OK", "> Account type: SPOT",
      "> Verifying key ownership...", "> Signature check: PASSED", "> Connection verified \u2713",
    ];
    lines.forEach((l, i) => {
      setTimeout(() => { setLog(prev => [...prev, l]); if (i === lines.length - 1) setVerifyStatus("ok"); }, 400 + i * 350);
    });
  }

  const perms = [
    { key: "read",    label: "Read account info",    required: true,  enabled: true },
    { key: "spot",    label: "Spot & margin trading", required: false, enabled: false },
    { key: "futures", label: "Futures trading",       required: false, enabled: false },
    { key: "withdraw",label: "Enable withdrawals",    required: false, enabled: false },
  ];

  const keysValid = exName.length > 0 && apiKey.length > 10 && secretKey.length > 10;

  return (
    <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 5, padding: "20px 22px", marginTop: 14 }}>
      <div style={{ marginBottom: 14 }}>
        <span style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>Link a new exchange</span>
      </div>

      <StepBar current={step} />

      {step === 1 && (
        <div>
          {[{ label: "EXCHANGE NAME", type: "text", placeholder: "e.g. Binance, Bybit, OKX", value: exName, onChange: setExName },
            { label: "API KEY", type: "text", placeholder: "Your API key", value: apiKey, onChange: setApiKey },
            { label: "SECRET KEY", type: "password", placeholder: "Your secret key", value: secretKey, onChange: setSecretKey }].map(f => (
            <div key={f.label} style={{ marginBottom: 10 }}>
              <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>{f.label}</div>
              <input type={f.type} placeholder={f.placeholder} value={f.value} onChange={e => f.onChange(e.target.value)}
                style={{ width: "100%", background: "var(--bg3)", border: "1px solid var(--line)", borderRadius: 3, padding: "9px 12px", color: "var(--t0)", fontSize: 10, outline: "none" }}
                onFocus={e => (e.target.style.borderColor = "var(--green)")} onBlur={e => (e.target.style.borderColor = "var(--line)")} />
            </div>
          ))}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 10 }}>
            <button onClick={onCancel}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={{ background: "transparent", border: "none", color: "var(--t2)", fontSize: 9, cursor: "pointer", transition: "color 0.15s ease" }}>{"\u2190"} Back</button>
            <button onClick={() => { if (keysValid) setStep(2); }} disabled={!keysValid} style={{
              padding: "10px 20px",
              background: keysValid ? "var(--green)" : "var(--bg2)", color: keysValid ? "var(--bg0)" : "var(--t2)",
              border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
              cursor: keysValid ? "pointer" : "not-allowed",
            }}>NEXT &rarr;</button>
          </div>
        </div>
      )}

      {step === 2 && (
        <div>
          <div ref={logRef} style={{
            background: "var(--bg0)", border: "1px solid var(--line)", borderRadius: 3,
            padding: 12, height: 140, overflowY: "auto", marginBottom: 14, fontSize: 10, lineHeight: 1.9,
          }}>
            {log.length === 0 && <span style={{ color: "var(--t2)" }}>_ awaiting verification</span>}
            {log.map((l, i) => (<div key={i} style={{ color: l.includes("\u2713") || l.includes("PASSED") ? "var(--green)" : "var(--t2)" }}>{l}</div>))}
            {verifyStatus === "checking" && <span style={{ color: "var(--t2)", animation: "blink-cursor 1s step-end infinite" }}>{"\u258C"}</span>}
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button onClick={() => { setStep(1); setVerifyStatus("idle"); setLog([]); }}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={{ background: "transparent", border: "none", color: "var(--t2)", fontSize: 9, cursor: "pointer", transition: "color 0.15s ease" }}>{"\u2190"} Back</button>
            <div>
              {verifyStatus === "idle" && <button onClick={runVerify} style={{ padding: "10px 20px", background: "var(--green)", color: "var(--bg0)", border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>RUN VERIFICATION</button>}
              {verifyStatus === "checking" && <span style={{ fontSize: 10, color: "var(--t2)" }}>Verifying...</span>}
              {verifyStatus === "ok" && <button onClick={() => setStep(3)} style={{ padding: "10px 20px", background: "var(--green)", color: "var(--bg0)", border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>NEXT &rarr;</button>}
            </div>
          </div>
        </div>
      )}

      {step === 3 && (
        <div>
          <div style={{ display: "flex", flexDirection: "column", gap: 5, marginBottom: 14 }}>
            {perms.map(p => (
              <div key={p.key} style={{
                display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px",
                background: p.required ? "var(--green-dim)" : "var(--bg2)",
                border: `1px solid ${p.required ? "var(--green-mid)" : "var(--line)"}`, borderRadius: 3,
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <div style={{ width: 5, height: 5, borderRadius: "50%", background: p.enabled ? "var(--green)" : "var(--line)" }} />
                  <span style={{ fontSize: 10, color: p.required ? "var(--t1)" : "var(--t2)" }}>{p.label}</span>
                </div>
                <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: p.enabled ? (p.required ? "var(--green)" : "var(--red)") : "var(--t2)" }}>
                  {p.enabled ? (p.required ? "REQUIRED" : "\u26A0 ENABLED") : "DISABLED"}
                </span>
              </div>
            ))}
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button onClick={() => setStep(2)}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={{ background: "transparent", border: "none", color: "var(--t2)", fontSize: 9, cursor: "pointer", transition: "color 0.15s ease" }}>{"\u2190"} Back</button>
            <button onClick={() => setStep(4)} style={{ padding: "10px 20px", background: "var(--green)", color: "var(--bg0)", border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>PERMISSIONS OK &rarr;</button>
          </div>
        </div>
      )}

      {step === 4 && (
        <div style={{ padding: "16px 0" }}>
          <div style={{ textAlign: "center", marginBottom: 14 }}>
            <div style={{ fontSize: 48, marginBottom: 10, color: "var(--green)" }}>{"\u2713"}</div>
            <div style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)", marginBottom: 6 }}>{exName} connected</div>
            <div style={{ fontSize: 10, color: "var(--t2)" }}>Read-only access verified.</div>
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button onClick={() => setStep(3)}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={{ background: "transparent", border: "none", color: "var(--t2)", fontSize: 9, cursor: "pointer", transition: "color 0.15s ease" }}>{"\u2190"} Back</button>
            <button onClick={() => {
              const id = exName.toLowerCase().replace(/\s+/g, "-") + "-" + Date.now();
              onComplete({ id, name: exName, maskedKey: mask(apiKey), lastSynced: "just now", balance: exName === "Binance" ? 127369 : 92210 });
            }} style={{ padding: "10px 24px", background: "var(--green)", color: "var(--bg0)", border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>DONE</button>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function SettingsPage() {
  const router = useRouter();
  const { exchanges, instances, addExchange, removeExchange } = useTrader();
  const [showWizard, setShowWizard] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState<string | null>(null);
  const [blockedRemove, setBlockedRemove] = useState<string | null>(null);

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 640, margin: "0 auto" }}>

        {/* Existing exchanges — collapse when wizard open */}
        <div style={{ opacity: showWizard ? 0.4 : 1, transition: "opacity 0.2s ease" }}>
          <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginBottom: 12 }}>
            LINKED EXCHANGES
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: showWizard ? 4 : 10 }}>
            {exchanges.map(ex => showWizard ? (
              /* Minimal single-line format when wizard is open */
              <div key={ex.id} style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "7px 14px", display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 10, fontWeight: 700, color: "var(--t1)" }}>{ex.name}</span>
                <span style={{ fontSize: 8, fontWeight: 700, letterSpacing: "0.1em", padding: "2px 6px", borderRadius: 3, background: "var(--green-dim)", color: "var(--green)", border: "1px solid var(--green-mid)" }}>CONNECTED</span>
                <span style={{ fontSize: 9, color: "var(--t2)" }}>{ex.maskedKey}</span>
              </div>
            ) : (
              /* Full expanded format */
              <div key={ex.id} style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 18px" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <span style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>{ex.name}</span>
                    <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", padding: "3px 8px", borderRadius: 3, background: "var(--green-dim)", color: "var(--green)", border: "1px solid var(--green-mid)" }}>CONNECTED</span>
                  </div>
                  {confirmRemove === ex.id ? (
                    <div style={{ display: "flex", gap: 6 }}>
                      <button onClick={() => { removeExchange(ex.id); setConfirmRemove(null); }}
                        style={{ background: "var(--red)", color: "var(--bg0)", border: "none", borderRadius: 3, padding: "4px 10px", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", cursor: "pointer" }}>CONFIRM</button>
                      <button onClick={() => setConfirmRemove(null)}
                        style={{ background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, padding: "4px 10px", fontSize: 9, cursor: "pointer" }}>CANCEL</button>
                    </div>
                  ) : (
                    <button onClick={() => {
                      const liveOnExchange = instances.filter(i => i.exchangeName === ex.name && i.status === "live");
                      if (liveOnExchange.length > 0) {
                        setBlockedRemove(ex.id);
                        setConfirmRemove(null);
                      } else {
                        setConfirmRemove(ex.id);
                        setBlockedRemove(null);
                      }
                    }}
                      onMouseEnter={e => { e.currentTarget.style.color = "var(--red)"; }}
                      onMouseLeave={e => { e.currentTarget.style.color = "var(--t3)"; }}
                      style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer", transition: "color 0.15s ease" }}>REMOVE</button>
                  )}
                </div>
                <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t1)" }}>
                  <span>{ex.maskedKey}</span>
                  <span style={{ color: "var(--t2)" }}>read-only</span>
                  <span style={{ color: "var(--t2)" }}>synced {ex.lastSynced}</span>
                </div>
                {blockedRemove === ex.id && (() => {
                  const liveOnExchange = instances.filter(i => i.exchangeName === ex.name && i.status === "live");
                  return (
                    <div style={{
                      background: "var(--bg2)", border: "0.5px solid #ff4d4d30", borderRadius: 6,
                      padding: "10px 12px", marginTop: 10,
                    }}>
                      <div style={{ fontSize: 10, color: "var(--t2)", lineHeight: 1.6, marginBottom: 8 }}>
                        This exchange is used by {liveOnExchange.length} active trader{liveOnExchange.length > 1 ? "s" : ""}. Pause or remove them before unlinking.
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 8 }}>
                        {liveOnExchange.map(inst => (
                          <span
                            key={inst.id}
                            onClick={() => router.push(`/trader/traders/${inst.id}`)}
                            style={{ fontSize: 10, color: "var(--t1)", cursor: "pointer", transition: "color 0.15s ease" }}
                            onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; e.currentTarget.style.textDecoration = "underline"; }}
                            onMouseLeave={e => { e.currentTarget.style.color = "var(--t1)"; e.currentTarget.style.textDecoration = "none"; }}
                          >{inst.strategyName} &middot; {inst.exchangeName} &rarr;</span>
                        ))}
                      </div>
                      <button onClick={() => setBlockedRemove(null)} style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0 }}>Cancel</button>
                    </div>
                  );
                })()}
              </div>
            ))}
            {exchanges.length === 0 && !showWizard && (
              <div style={{ padding: "24px 0", textAlign: "center", color: "var(--t2)", fontSize: 10 }}>No exchanges linked yet.</div>
            )}
          </div>
        </div>

        {/* Link new exchange button / wizard */}
        {!showWizard ? (
          <button onClick={() => setShowWizard(true)} style={{
            marginTop: 14, width: "100%", padding: "10px 0",
            background: "transparent", color: "var(--green)",
            border: "1px dashed var(--line2)", borderRadius: 5,
            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer",
          }}>+ LINK A NEW EXCHANGE</button>
        ) : (
          <LinkWizard onComplete={ex => { addExchange(ex); setShowWizard(false); setConfirmRemove(null); }} onCancel={() => setShowWizard(false)} />
        )}
      </div>
    </div>
  );
}

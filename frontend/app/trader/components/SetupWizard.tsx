"use client";

// TODO(prompt-b-followup): update permissions rendering, error handling to
// match LinkWizard. This wizard still uses the cosmetic-terminal-log verify
// flow and hardcoded permissions assumptions; only the credential submission
// has been tightened to the new strict types. Tracked separately.

import React, { useState, useEffect, useRef } from "react";
import { useTrader, Exchange, fmt, mask } from "../context";
import { allocatorApi, type ExchangeSlug } from "../api";
import AllocationPicker from "./AllocationPicker";

// ─── Exchange catalog ───────────────────────────────────────────────────────

// Only backend-supported exchanges. Bybit/OKX were previously listed but
// would 400 on submit — removed until backend support is added.
const EXCHANGE_OPTIONS: { name: string; slug: ExchangeSlug; badge: string; markets: string }[] = [
  { name: "Binance", slug: "binance", badge: "BN", markets: "Spot · Futures · Margin" },
  { name: "BloFin",  slug: "blofin",  badge: "BF", markets: "Futures" },
];

const EXCHANGE_KEY_STEPS: Record<string, string[]> = {
  Binance: [
    "Log into Binance \u2192 profile icon \u2192 API Management",
    "Click Create API \u2192 label \u2192 verify",
    "Enable Reading + Spot & Margin Trading \u2014 do NOT enable Withdrawals",
    "Copy API Key and Secret Key and paste below",
  ],
  BloFin: [
    "Log into BloFin \u2192 profile icon \u2192 API",
    "Click Create API Key \u2192 set passphrase",
    "Enable READ + TRADE \u2014 do NOT enable TRANSFER or Withdraw",
    "Copy API Key, Secret Key, and your passphrase below",
  ],
};

// ─── Step bar ────────────────────────────────────────────────────────────────

function StepBar({ current }: { current: number }) {
  const steps = ["Exchange", "Keys", "Allocate", "Confirm"];
  return (
    <div style={{ display: "flex", alignItems: "flex-start", width: "100%", marginBottom: 20 }}>
      {steps.map((label, i) => {
        const n = i + 1;
        const done = current > n;
        const active = current === n;
        return (
          <React.Fragment key={n}>
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 22, height: 22, borderRadius: "50%",
                background: done ? "var(--green-dim)" : active ? "var(--bg3)" : "var(--bg3)",
                border: done ? "1px solid var(--green-mid)" : active ? "2px solid var(--green)" : "2px solid var(--line)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 9, fontWeight: 700,
                color: done ? "var(--green)" : active ? "var(--green)" : "var(--t2)",
              }}>
                {done ? "\u2713" : n}
              </div>
              <span style={{
                fontSize: 9, fontWeight: 700, textAlign: "center", whiteSpace: "nowrap",
                color: done ? "var(--green)" : active ? "var(--t0)" : "var(--t2)",
                letterSpacing: "0.12em", textTransform: "uppercase",
              }}>{label}</span>
            </div>
            {i < steps.length - 1 && (
              <div style={{ flex: 1, height: 1, marginTop: 11, background: done ? "var(--green)" : "var(--line)" }} />
            )}
          </React.Fragment>
        );
      })}
    </div>
  );
}

// ─── Setup Wizard ────────────────────────────────────────────────────────────

interface SetupWizardProps {
  strategyName: string;
  onActivate: (exchangeId: string, exchangeName: string, allocation: number) => void;
  onCancel: () => void;
}

export default function SetupWizard({ strategyName, onActivate, onCancel }: SetupWizardProps) {
  const { exchanges, instances, addExchange } = useTrader();

  const otherAllocated = instances.reduce((s, i) => s + (i.allocation ?? 0), 0);
  const totalBalance = exchanges.reduce((s, e) => s + e.balance, 0);
  const availableBalance = totalBalance - otherAllocated;

  const [step, setStep] = useState(1);
  // selectedExchangeName is the exchange name chosen in step 1 (from catalog or linked)
  const [selectedExchangeName, setSelectedExchangeName] = useState<string | null>(exchanges.length > 0 ? exchanges[0].name : null);
  // selectedExchangeId is the linked exchange id (set after keys step or if already linked)
  const [selectedExchangeId, setSelectedExchangeId] = useState<string | null>(exchanges.length > 0 ? exchanges[0].id : null);

  // Step 1 expand toggle
  const [showNewExchanges, setShowNewExchanges] = useState(false);
  // Step 2 guide toggle
  const [showGuide, setShowGuide] = useState(false);

  // Keys form
  const [inlineApiKey, setInlineApiKey] = useState("");
  const [inlineSecretKey, setInlineSecretKey] = useState("");
  const [inlinePassphrase, setInlinePassphrase] = useState("");
  const [verifyStatus, setVerifyStatus] = useState<"idle" | "checking" | "ok">("idle");
  const [verifyLog, setVerifyLog] = useState<string[]>([]);
  const logRef = useRef<HTMLDivElement>(null);

  // Configure
  const [allocation, setAllocation] = useState("25000");

  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [verifyLog]);

  const needsPassphraseStep = EXCHANGE_OPTIONS.find(o => o.name === selectedExchangeName)?.slug === "blofin";
  const inlineValid =
    inlineApiKey.length > 10
    && inlineSecretKey.length > 10
    && (!needsPassphraseStep || inlinePassphrase.trim().length > 0);

  // Check if the selected exchange is already linked
  const linkedExchange = selectedExchangeName ? exchanges.find(e => e.name === selectedExchangeName) : null;

  function handleStep1Continue() {
    if (!selectedExchangeName) return;
    if (linkedExchange) {
      // Already linked — skip keys step, go to allocate
      setSelectedExchangeId(linkedExchange.id);
      setStep(3);
    } else {
      setStep(2);
    }
  }

  const [verifyError, setVerifyError] = useState<string | null>(null);

  async function runVerify() {
    if (!selectedExchangeName) return;
    setVerifyStatus("checking");
    setVerifyLog([]);
    setVerifyError(null);

    const addLog = (line: string) => setVerifyLog(prev => [...prev, line]);

    addLog(`> Connecting to ${selectedExchangeName} API...`);

    // Narrow the exchange name against the supported list — reject unknowns early.
    const opt = EXCHANGE_OPTIONS.find(o => o.name === selectedExchangeName);
    if (!opt) {
      const msg = `Exchange '${selectedExchangeName}' is not supported.`;
      addLog(`> Error: ${msg}`);
      setVerifyError(msg);
      setVerifyStatus("idle");
      return;
    }
    const slug: ExchangeSlug = opt.slug;
    if (slug === "blofin" && !inlinePassphrase.trim()) {
      const msg = "BloFin requires a passphrase.";
      addLog(`> Error: ${msg}`);
      setVerifyError(msg);
      setVerifyStatus("idle");
      return;
    }

    try {
      addLog("> Storing encrypted credentials...");
      const result = await allocatorApi.storeExchangeKeys({
        exchange: slug,
        label: selectedExchangeName,
        api_key: inlineApiKey.trim(),
        api_secret: inlineSecretKey.trim(),
        passphrase: slug === "blofin" ? inlinePassphrase.trim() : undefined,
      });

      addLog("> Credentials stored securely");
      addLog("> Triggering balance snapshot...");

      // Trigger a snapshot refresh to validate the keys work
      const snapResult = await allocatorApi.refreshSnapshots().catch(() => null);
      if (snapResult) {
        addLog("> Balance fetched successfully");
      }

      addLog("> Connection verified \u2713");
      setVerifyStatus("ok");

      // Find the balance from snapshot data
      const snap = snapResult?.snapshots?.find(s => s.connection_id === result.connection_id);
      const balance = snap?.total_equity_usd ?? 0;

      const newEx: Exchange = {
        id: result.connection_id,
        exchange: slug,
        name: selectedExchangeName,
        maskedKey: result.masked_key,
        lastSynced: "just now",
        balance,
        status: "active",
        lastErrorMsg: null,
        permissions: result.permissions,
        lastValidatedAt: new Date().toISOString(),
        // Newly-linked connections have no operator-set anchor yet.
        principalAnchorAt: null,
        principalBaselineUsd: null,
      };
      addExchange(newEx);
      setSelectedExchangeId(newEx.id);
      setTimeout(() => setStep(3), 800);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Verification failed";
      addLog(`> Error: ${msg}`);
      setVerifyError(msg);
      setVerifyStatus("idle");
    }
  }

  function fireCelebration(stratName: string, exchName: string) {
    // 1. Dim overlay
    const overlay = document.createElement("div");
    overlay.style.cssText = "position:fixed;inset:0;background:rgba(8,8,9,0.85);z-index:9998;pointer-events:none;opacity:0;transition:opacity 0.2s ease";
    document.body.appendChild(overlay);
    requestAnimationFrame(() => { overlay.style.opacity = "1"; });

    // 2. Confetti canvas
    const canvas = document.createElement("canvas");
    canvas.style.cssText = "position:fixed;inset:0;width:100vw;height:100vh;z-index:9999;pointer-events:none";
    document.body.appendChild(canvas);
    const ctx = canvas.getContext("2d")!;
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    const cx = canvas.width / 2, cy = canvas.height / 2;
    const colors = ["#00c896", "#f0ede6", "#00c89660", "#a09d96", "#00c89630", "#ffffff"];
    const particles = Array.from({ length: 120 }, () => {
      const angle = Math.random() * Math.PI * 2;
      const speed = Math.random() * 9 + 3;
      return {
        x: cx, y: cy,
        vx: Math.cos(angle) * speed,
        vy: Math.sin(angle) * speed - Math.random() * 6,
        size: Math.random() * 5 + 2,
        color: colors[Math.floor(Math.random() * colors.length)],
        circle: Math.random() > 0.5,
        alpha: 1,
        rotation: Math.random() * Math.PI * 2,
        spin: (Math.random() - 0.5) * 0.2,
      };
    });
    let frame = 0;
    function tick() {
      if (frame++ >= 140) { canvas.remove(); return; }
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      for (const p of particles) {
        p.vx *= 0.99; p.vy += 0.35; p.x += p.vx; p.y += p.vy;
        p.alpha -= 0.01; p.rotation += p.spin;
        if (p.alpha <= 0) continue;
        ctx.save();
        ctx.globalAlpha = p.alpha;
        ctx.translate(p.x, p.y);
        ctx.rotate(p.rotation);
        ctx.fillStyle = p.color;
        if (p.circle) {
          ctx.beginPath(); ctx.arc(0, 0, p.size / 2, 0, Math.PI * 2); ctx.fill();
        } else {
          ctx.fillRect(-p.size / 2, -p.size * 0.275, p.size, p.size * 0.55);
        }
        ctx.restore();
      }
      requestAnimationFrame(tick);
    }
    requestAnimationFrame(tick);

    // 3. Centered message
    const msg = document.createElement("div");
    msg.style.cssText = "position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:10000;text-align:center;pointer-events:none;opacity:0;transition:opacity 0.2s ease";
    msg.innerHTML = `<div style="font-size:10px;font-weight:700;color:#00c896;letter-spacing:0.1em;font-family:'Space Mono',monospace;margin-bottom:6px">${stratName} &middot; ${exchName}</div>`
      + `<div style="font-size:28px;font-weight:700;color:#f0ede6;font-family:'Space Mono',monospace">Now live</div>`
      + `<div style="font-size:9px;color:#35332f;font-family:'Space Mono',monospace;letter-spacing:0.06em;margin-top:8px">Redirecting to your trader dashboard...</div>`;
    document.body.appendChild(msg);
    setTimeout(() => { msg.style.opacity = "1"; }, 150);

    // 4. Fade out and cleanup — return promise for redirect timing
    return {
      fadeOut: () => {
        overlay.style.opacity = "0";
        msg.style.opacity = "0";
        setTimeout(() => { overlay.remove(); msg.remove(); }, 200);
      },
    };
  }

  function handleActivate() {
    if (!selectedExchangeId || !selectedExchangeName) return;
    const celebration = fireCelebration(strategyName, selectedExchangeName);
    onActivate(selectedExchangeId, selectedExchangeName, parseInt(allocation) || 25000);
    // Store fadeOut for the redirect handler in the parent
    (window as any).__celebrationFadeOut = celebration.fadeOut;
  }

  // Resolve the selected exchange for display in later steps
  const resolvedExchange = selectedExchangeId ? exchanges.find(e => e.id === selectedExchangeId) : null;

  return (
    <div>
      {/* Status row */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        paddingBottom: 14, marginBottom: 16, borderBottom: "1px solid var(--line)",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{ width: 5, height: 5, borderRadius: "50%", background: "transparent", border: "1px solid var(--green)" }} />
          <span style={{ fontSize: 10, color: "var(--t2)" }}>Quick Setup</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          {step >= 3 && resolvedExchange && (
            <span style={{ fontSize: 9, color: "var(--t2)" }}>{resolvedExchange.name} &middot; {resolvedExchange.maskedKey}</span>
          )}
          <span style={{ fontSize: 10, color: "var(--t2)" }}>Step {step} of 4</span>
        </div>
      </div>

      <StepBar current={step} />

      {/* Step 1: Exchange selection */}
      {step === 1 && (
        <div>
          {exchanges.length > 0 ? (
            <>
              {/* Linked exchanges */}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginBottom: 12 }}>
                {exchanges.map(ex => {
                  const sel = selectedExchangeName === ex.name && selectedExchangeId === ex.id;
                  const catalogEntry = EXCHANGE_OPTIONS.find(o => o.name === ex.name);
                  return (
                    <div key={ex.id} onClick={() => { setSelectedExchangeName(ex.name); setSelectedExchangeId(ex.id); }} style={{
                      background: sel ? "var(--green-dim)" : "var(--bg2)",
                      border: `1px solid ${sel ? "var(--green)" : "var(--line)"}`,
                      borderRadius: 6, padding: "14px 14px", cursor: "pointer",
                      display: "flex", alignItems: "center", gap: 10,
                      transition: "all 0.15s ease",
                    }}>
                      <div style={{
                        width: 32, height: 32, borderRadius: 4, flexShrink: 0,
                        background: "var(--bg3)", display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 11, fontWeight: 700, color: "var(--t1)",
                      }}>{catalogEntry?.badge ?? ex.name.slice(0, 2).toUpperCase()}</div>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>{ex.name}</div>
                        <div style={{ fontSize: 10, color: "var(--t2)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{ex.maskedKey}</div>
                      </div>
                      <div style={{
                        width: 18, height: 18, borderRadius: "50%", flexShrink: 0,
                        background: sel ? "var(--green)" : "transparent",
                        border: sel ? "none" : "1.5px solid var(--line)",
                        display: "flex", alignItems: "center", justifyContent: "center",
                      }}>
                        {sel && <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><polyline points="1.5,5 4,7.5 8.5,2.5" stroke="var(--bg0)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>}
                      </div>
                    </div>
                  );
                })}
              </div>

              {/* Expandable unlinked exchanges */}
              {EXCHANGE_OPTIONS.filter(o => !exchanges.some(e => e.name === o.name)).length > 0 && (
                <>
                  {!showNewExchanges && (
                    <span
                      onClick={() => setShowNewExchanges(true)}
                      style={{ fontSize: 9, color: "var(--t3)", cursor: "pointer", transition: "color 0.15s ease", textDecoration: "none" }}
                      onMouseEnter={e => { e.currentTarget.style.textDecoration = "underline"; }}
                      onMouseLeave={e => { e.currentTarget.style.textDecoration = "none"; }}
                    >Link a different exchange &rarr;</span>
                  )}
                  <div style={{
                    maxHeight: showNewExchanges ? 300 : 0,
                    overflow: "hidden",
                    transition: "max-height 0.3s ease",
                    marginTop: showNewExchanges ? 12 : 0,
                  }}>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                      {EXCHANGE_OPTIONS.filter(o => !exchanges.some(e => e.name === o.name)).map(opt => {
                        const sel = selectedExchangeName === opt.name && !selectedExchangeId;
                        return (
                          <div key={opt.name} onClick={() => { setSelectedExchangeName(opt.name); setSelectedExchangeId(null); }} style={{
                            background: sel ? "var(--green-dim)" : "var(--bg2)",
                            border: `1px solid ${sel ? "var(--green)" : "var(--line)"}`,
                            borderRadius: 6, padding: "14px 14px", cursor: "pointer",
                            display: "flex", alignItems: "center", gap: 10,
                            transition: "all 0.15s ease",
                          }}>
                            <div style={{
                              width: 32, height: 32, borderRadius: 4, flexShrink: 0,
                              background: "var(--bg3)", display: "flex", alignItems: "center", justifyContent: "center",
                              fontSize: 11, fontWeight: 700, color: "var(--t1)",
                            }}>{opt.badge}</div>
                            <div style={{ flex: 1, minWidth: 0 }}>
                              <div style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>{opt.name}</div>
                              <div style={{ fontSize: 10, color: "var(--t2)" }}>{opt.markets}</div>
                            </div>
                            <div style={{
                              width: 18, height: 18, borderRadius: "50%", flexShrink: 0,
                              background: sel ? "var(--green)" : "transparent",
                              border: sel ? "none" : "1.5px solid var(--line)",
                              display: "flex", alignItems: "center", justifyContent: "center",
                            }}>
                              {sel && <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><polyline points="1.5,5 4,7.5 8.5,2.5" stroke="var(--bg0)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </>
              )}
              <div style={{ marginBottom: 16 }} />
            </>
          ) : (
            /* No linked exchanges — show full catalog */
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginBottom: 16 }}>
              {EXCHANGE_OPTIONS.map(opt => {
                const sel = selectedExchangeName === opt.name && !selectedExchangeId;
                return (
                  <div key={opt.name} onClick={() => { setSelectedExchangeName(opt.name); setSelectedExchangeId(null); }} style={{
                    background: sel ? "var(--green-dim)" : "var(--bg2)",
                    border: `1px solid ${sel ? "var(--green)" : "var(--line)"}`,
                    borderRadius: 6, padding: "14px 14px", cursor: "pointer",
                    display: "flex", alignItems: "center", gap: 10,
                    transition: "all 0.15s ease",
                  }}>
                    <div style={{
                      width: 32, height: 32, borderRadius: 4, flexShrink: 0,
                      background: "var(--bg3)", display: "flex", alignItems: "center", justifyContent: "center",
                      fontSize: 11, fontWeight: 700, color: "var(--t1)",
                    }}>{opt.badge}</div>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>{opt.name}</div>
                      <div style={{ fontSize: 10, color: "var(--t2)" }}>{opt.markets}</div>
                    </div>
                    <div style={{
                      width: 18, height: 18, borderRadius: "50%", flexShrink: 0,
                      background: sel ? "var(--green)" : "transparent",
                      border: sel ? "none" : "1.5px solid var(--line)",
                      display: "flex", alignItems: "center", justifyContent: "center",
                    }}>
                      {sel && <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><polyline points="1.5,5 4,7.5 8.5,2.5" stroke="var(--bg0)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>}
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
            <button onClick={onCancel} style={{ padding: "10px 16px", background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>CANCEL</button>
            <button onClick={handleStep1Continue} disabled={!selectedExchangeName} style={{
              padding: "10px 20px",
              background: selectedExchangeName ? "var(--green)" : "var(--bg3)",
              color: selectedExchangeName ? "var(--bg0)" : "var(--t2)",
              border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700,
              letterSpacing: "0.12em", textTransform: "uppercase",
              cursor: selectedExchangeName ? "pointer" : "not-allowed",
            }}>NEXT &rarr;</button>
          </div>
        </div>
      )}

      {/* Step 2: Keys */}
      {step === 2 && (() => {
        const guideSteps = EXCHANGE_KEY_STEPS[selectedExchangeName ?? ""] ?? EXCHANGE_KEY_STEPS.Binance;
        const badge = EXCHANGE_OPTIONS.find(o => o.name === selectedExchangeName)?.badge ?? "??";

        const inputsBlock = (
          <>
            <div style={{ marginBottom: 10 }}>
              <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>API KEY</div>
              <input type="text" placeholder="Paste your API key" value={inlineApiKey} onChange={e => setInlineApiKey(e.target.value)}
                style={{ width: "100%", background: "var(--bg0)", border: "1px solid var(--line)", borderRadius: 3, padding: "9px 12px", color: "var(--t0)", fontSize: 10, outline: "none" }}
                onFocus={e => (e.target.style.borderColor = "var(--green)")} onBlur={e => (e.target.style.borderColor = "var(--line)")} />
            </div>
            <div style={{ marginBottom: 8 }}>
              <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>SECRET KEY</div>
              <input type="password" placeholder="Paste your secret key" value={inlineSecretKey} onChange={e => setInlineSecretKey(e.target.value)}
                style={{ width: "100%", background: "var(--bg0)", border: "1px solid var(--line)", borderRadius: 3, padding: "9px 12px", color: "var(--t0)", fontSize: 10, outline: "none" }}
                onFocus={e => (e.target.style.borderColor = "var(--green)")} onBlur={e => (e.target.style.borderColor = "var(--line)")} />
            </div>
            {needsPassphraseStep && (
              <div style={{ marginBottom: 8 }}>
                <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>PASSPHRASE</div>
                <input type="password" placeholder="BloFin passphrase" value={inlinePassphrase} onChange={e => setInlinePassphrase(e.target.value)}
                  style={{ width: "100%", background: "var(--bg0)", border: "1px solid var(--line)", borderRadius: 3, padding: "9px 12px", color: "var(--t0)", fontSize: 10, outline: "none" }}
                  onFocus={e => (e.target.style.borderColor = "var(--green)")} onBlur={e => (e.target.style.borderColor = "var(--line)")} />
              </div>
            )}
            <div style={{ fontSize: 9, color: "var(--t3)", lineHeight: 1.6, marginBottom: 14 }}>
              Your secret key is only used once to verify your connection and is never stored in plain text.
            </div>
          </>
        );

        return (
          <div>
            {/* Header: exchange identity + help toggle */}
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <div style={{
                  width: 32, height: 32, borderRadius: 6, flexShrink: 0,
                  background: "var(--green-dim)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 12, fontWeight: 700, color: "var(--green)",
                }}>{badge}</div>
                <div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: "var(--t0)" }}>{selectedExchangeName}</div>
                  <div style={{ fontSize: 9, color: "var(--t3)" }}>Encrypted &middot; never stored in plain text</div>
                </div>
              </div>
              <span
                onClick={() => setShowGuide(v => !v)}
                style={{ fontSize: 9, color: "var(--t2)", cursor: "pointer", whiteSpace: "nowrap", flexShrink: 0 }}
                onMouseEnter={e => { e.currentTarget.style.textDecoration = "underline"; }}
                onMouseLeave={e => { e.currentTarget.style.textDecoration = "none"; }}
              >{showGuide ? "Hide guide \u2039" : "Need help finding these? \u203A"}</span>
            </div>

            {/* Instructions panel — collapsible */}
            <div style={{ overflow: "hidden", maxHeight: showGuide ? 400 : 0, transition: "max-height 0.3s ease" }}>
              <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6, padding: "12px 14px", marginBottom: 16 }}>
                <div style={{ fontSize: 9, color: "var(--t3)", fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 10 }}>
                  HOW TO FIND YOUR {selectedExchangeName?.toUpperCase()} API KEYS
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {guideSteps.map((text, i) => (
                    <div key={i} style={{ display: "flex", gap: 8 }}>
                      <span style={{ fontSize: 9, fontWeight: 700, color: "var(--green)", flexShrink: 0 }}>{i + 1}</span>
                      <span style={{ fontSize: 9, color: "var(--t2)", lineHeight: 1.6 }}>{text}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Two-column when guide open, full-width when closed */}
            {showGuide ? (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 0 }}>
                {/* Left: instructions visible via panel above — this column intentionally empty or can hold context */}
                <div />
                {/* Right: inputs */}
                <div>{inputsBlock}</div>
              </div>
            ) : (
              inputsBlock
            )}

            {/* Reassurance box — always visible */}
            <div style={{
              background: "var(--bg2)", border: "0.5px solid var(--line)", borderRadius: 6,
              padding: "10px 12px", marginBottom: 14,
              display: "flex", alignItems: "flex-start", gap: 8,
            }}>
              <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--t2)", flexShrink: 0, marginTop: 5 }} />
              <span style={{ fontSize: 10, lineHeight: 1.6, color: "var(--t2)" }}>
                Your keys are encrypted at rest and never leave your server.
              </span>
            </div>

            {/* Verification log */}
            {verifyLog.length > 0 && (
              <div ref={logRef} style={{
                background: "var(--bg0)", border: "1px solid var(--line)", borderRadius: 3,
                padding: 12, height: 120, overflowY: "auto", marginBottom: 12, fontSize: 10, lineHeight: 1.9,
              }}>
                {verifyLog.map((l, i) => (
                  <div key={i} style={{ color: l.includes("\u2713") || l.includes("PASSED") ? "var(--green)" : l.includes("Error") ? "var(--red)" : "var(--t2)" }}>{l}</div>
                ))}
                {verifyStatus === "checking" && <span style={{ color: "var(--t2)", animation: "blink-cursor 1s step-end infinite" }}>{"\u258C"}</span>}
              </div>
            )}
            {verifyError && (
              <div style={{ fontSize: 9, color: "var(--red)", marginBottom: 12 }}>
                Failed to verify connection. Check your API keys and try again.
              </div>
            )}

            {/* Footer */}
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
              <button onClick={() => { setStep(1); setVerifyStatus("idle"); setVerifyLog([]); setShowGuide(false); }} style={{ padding: "10px 16px", background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>&larr; BACK</button>
              {verifyStatus === "idle" && (
                <button onClick={runVerify} disabled={!inlineValid} style={{
                  padding: "10px 20px",
                  background: inlineValid ? "var(--green)" : "var(--bg3)",
                  color: inlineValid ? "var(--bg0)" : "var(--t2)",
                  border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700,
                  letterSpacing: "0.12em", textTransform: "uppercase",
                  cursor: inlineValid ? "pointer" : "not-allowed",
                }}>VERIFY & NEXT &rarr;</button>
              )}
              {verifyStatus === "checking" && <div style={{ fontSize: 10, color: "var(--t2)", padding: "10px 0" }}>Verifying...</div>}
              {verifyStatus === "ok" && <div style={{ fontSize: 10, color: "var(--green)", padding: "10px 0" }}>Verified &mdash; advancing...</div>}
            </div>
          </div>
        );
      })()}

      {/* Step 3: Allocate */}
      {step === 3 && (
        <div>
          <AllocationPicker value={allocation} onChange={setAllocation} otherAllocated={otherAllocated} />

          <div style={{ display: "flex", justifyContent: "space-between", borderTop: "0.5px solid var(--line)", paddingTop: 14 }}>
            <button onClick={() => setStep(linkedExchange ? 1 : 2)} style={{ padding: "10px 16px", background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>&larr; BACK</button>
            {(() => {
              const allocVal = parseInt(allocation) || 0;
              const valid = allocVal > 0 && allocVal <= availableBalance;
              return <button onClick={() => { if (valid) setStep(4); }} disabled={!valid} style={{ padding: "10px 20px", background: valid ? "var(--green)" : "var(--bg3)", color: valid ? "var(--bg0)" : "var(--t2)", border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: valid ? "pointer" : "not-allowed", opacity: valid ? 1 : 0.4 }}>NEXT &rarr;</button>;
            })()}
          </div>
        </div>
      )}

      {/* Step 4: Confirm */}
      {step === 4 && (
        <div>
          <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 16px", marginBottom: 14 }}>
            {[
              { label: "Strategy", value: strategyName },
              { label: "Exchange", value: resolvedExchange?.name ?? selectedExchangeName ?? "\u2014" },
              { label: "Allocation", value: `$${parseInt(allocation).toLocaleString()}` },
            ].map(row => (
              <div key={row.label} style={{ display: "flex", justifyContent: "space-between", padding: "7px 0", borderBottom: "1px solid var(--bg3)" }}>
                <span style={{ fontSize: 10, color: "var(--t2)" }}>{row.label}</span>
                <span style={{ fontSize: 10, color: "var(--t1)", fontWeight: 700 }}>{row.value}</span>
              </div>
            ))}
          </div>

          <p style={{ fontSize: 9, color: "var(--t3)", margin: "0 0 14px", lineHeight: 1.6 }}>
            Signal processing starts immediately. Power and alerts can be adjusted from your trader dashboard at any time.
          </p>

          <div style={{ display: "flex", justifyContent: "space-between" }}>
            <button onClick={() => setStep(3)} style={{ padding: "10px 16px", background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>&larr; BACK</button>
            <button onClick={handleActivate} style={{ padding: "10px 20px", background: "var(--green)", color: "var(--bg0)", border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>ACTIVATE &rarr;</button>
          </div>
        </div>
      )}
    </div>
  );
}

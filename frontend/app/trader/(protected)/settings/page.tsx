"use client";

import { useState, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import { useTrader, Exchange } from "../../context";
import {
  allocatorApi,
  parseApiError,
  type ExchangeSlug,
  type ExchangePermissions,
  type StoreKeysSuccess,
} from "../../api";

const SUPPORTED_EXCHANGES: ExchangeSlug[] = ["binance", "blofin"];
const VERIFY_STEP_LOG_LINES = [
  "> Connecting to exchange API...",
  "> Verifying key ownership...",
  "> Checking permissions...",
];

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

// ─── Shared wizard styling ───────────────────────────────────────────────────

const fieldLabelStyle: React.CSSProperties = {
  fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em",
  fontWeight: 700, textTransform: "uppercase", marginBottom: 6,
};

const inputStyle: React.CSSProperties = {
  width: "100%", background: "var(--bg3)", border: "1px solid var(--line)",
  borderRadius: 3, padding: "9px 12px", color: "var(--t0)",
  fontSize: 10, outline: "none",
  fontFamily: "var(--font-space-mono), Space Mono, monospace",
};

const primaryBtnStyle: React.CSSProperties = {
  padding: "10px 20px", background: "var(--green)", color: "var(--bg0)",
  border: "none", borderRadius: 3, fontSize: 9, fontWeight: 700,
  letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer",
};

const disabledBtnStyle: React.CSSProperties = {
  ...primaryBtnStyle,
  background: "var(--bg2)", color: "var(--t2)", cursor: "not-allowed",
};

const textBtnStyle: React.CSSProperties = {
  background: "transparent", border: "none", color: "var(--t2)",
  fontSize: 9, cursor: "pointer", transition: "color 0.15s ease",
};

const inlineErrorStyle: React.CSSProperties = {
  fontSize: 9, color: "var(--red)", marginTop: 4, letterSpacing: "0.02em",
};

// ─── Permissions step (live data from backend) ───────────────────────────────

// Step 3 is INFORMATIONAL, not a gate. If the backend returned 200 on step 2,
// the key was already accepted by policy — step 3 just surfaces what it can do.
// Per-exchange role map:
//   required: must be ENABLED for the account type
//   allowed:  can be either; informational
//   rejected: must be DISABLED (backend already enforces)
//   inferred: BloFin's query-apikey can't distinguish this permission — we trust
//             the user's key configuration and surface that ambiguity explicitly

type RoleName = "required" | "allowed" | "rejected" | "inferred";

type RowSpec = {
  key: keyof ExchangePermissions;
  label: string;
  binance: Exclude<RoleName, "inferred">;
  blofin:  RoleName;
};

const PERMISSION_ROWS: RowSpec[] = [
  { key: "read",          label: "Read account info",     binance: "required", blofin: "required" },
  { key: "spot_trade",    label: "Spot & margin trading", binance: "required", blofin: "inferred" },
  { key: "futures_trade", label: "Futures trading",       binance: "allowed",  blofin: "inferred" },
  { key: "withdrawals",   label: "Enable withdrawals",    binance: "rejected", blofin: "inferred" },
];

const BLOFIN_INFERRED_TOOLTIP =
  "BloFin doesn't distinguish trade from withdrawal permissions via API. " +
  "Verify your key is Trade-only on BloFin's side.";

function PermissionsStep({
  exchange, permissions,
}: {
  exchange: string;
  permissions: ExchangePermissions | null;
}) {
  return (
    <div>
      <div style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase", marginBottom: 10,
      }}>
        Key permissions
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 5, marginBottom: 14 }}>
        {PERMISSION_ROWS.map(row => {
          const role: RoleName = exchange === "blofin" ? row.blofin : row.binance;
          const value = permissions?.[row.key] ?? null;
          // Resolve pill + row bg by role × value.
          let pillColor: string, pillBg: string, pillBorder: string;
          let pillText: string;
          let tooltip: string | undefined;
          let isGoodState = false;

          if (role === "required") {
            if (value === true) {
              pillColor = "var(--green)"; pillBg = "var(--green-dim)"; pillBorder = "var(--green-mid)";
              pillText = "ACTIVE"; isGoodState = true;
            } else if (value === false) {
              pillColor = "var(--red)"; pillBg = "var(--red-dim)"; pillBorder = "var(--red)";
              pillText = "MISSING";
            } else {
              pillColor = "var(--amber)"; pillBg = "var(--amber-dim)"; pillBorder = "var(--amber)";
              pillText = "UNKNOWN";
            }
          } else if (role === "allowed") {
            if (value === true) {
              pillColor = "var(--t1)"; pillBg = "var(--bg2)"; pillBorder = "var(--line2)";
              pillText = "ENABLED";
            } else if (value === false) {
              pillColor = "var(--t2)"; pillBg = "var(--bg2)"; pillBorder = "var(--line)";
              pillText = "DISABLED";
            } else {
              pillColor = "var(--t2)"; pillBg = "var(--bg2)"; pillBorder = "var(--line)";
              pillText = "UNKNOWN";
            }
          } else if (role === "rejected") {
            if (value === true) {
              pillColor = "var(--red)"; pillBg = "var(--red-dim)"; pillBorder = "var(--red)";
              pillText = "ENABLED — SHOULD NOT BE";
            } else if (value === false) {
              pillColor = "var(--green)"; pillBg = "var(--green-dim)"; pillBorder = "var(--green-mid)";
              pillText = "DISABLED"; isGoodState = true;
            } else {
              pillColor = "var(--amber)"; pillBg = "var(--amber-dim)"; pillBorder = "var(--amber)";
              pillText = "UNKNOWN";
            }
          } else {
            // inferred (BloFin) — API can't distinguish trade from transfer/withdraw.
            // Treat true and null both as ambiguous "trust setup" since readOnly=0
            // means the key is non-read-only but we can't prove which category.
            if (value === true || value === null) {
              pillColor = "var(--amber)"; pillBg = "var(--amber-dim)"; pillBorder = "var(--amber)";
              pillText = "UNCLEAR — TRUST SETUP";
              tooltip = BLOFIN_INFERRED_TOOLTIP;
            } else {
              pillColor = "var(--t2)"; pillBg = "var(--bg2)"; pillBorder = "var(--line)";
              pillText = "NOT APPLICABLE";
            }
          }

          const rowBg = isGoodState ? "var(--green-dim)" : "var(--bg2)";
          const rowBorder = isGoodState ? "var(--green-mid)" : "var(--line)";
          const dotColor = value === true ? pillColor : "var(--line)";
          return (
            <div key={row.key} style={{
              display: "flex", alignItems: "center", justifyContent: "space-between",
              padding: "8px 12px", background: rowBg,
              border: `1px solid ${rowBorder}`, borderRadius: 3,
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ width: 5, height: 5, borderRadius: "50%", background: dotColor }} />
                <span style={{ fontSize: 10, color: role === "required" ? "var(--t1)" : "var(--t2)" }}>
                  {row.label}
                </span>
              </div>
              <span
                title={tooltip}
                style={{
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  color: pillColor, background: pillBg,
                  border: `1px solid ${pillBorder}`, borderRadius: 3, padding: "2px 7px",
                  cursor: tooltip ? "help" : "default",
                }}
              >
                {pillText}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─── Link wizard ─────────────────────────────────────────────────────────────

type VerifyState =
  | { kind: "idle" }
  | { kind: "checking"; visibleLines: number }
  | { kind: "ok"; result: StoreKeysSuccess }
  | { kind: "err"; status: number; detail: string };

function LinkWizard({ onComplete, onCancel }: { onComplete: (connectionId: string) => void; onCancel: () => void }) {
  const [step, setStep] = useState<1 | 2 | 3 | 4>(1);

  // Step 1 form state
  const [exchange, setExchange] = useState<"" | ExchangeSlug>("");
  const [label, setLabel] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [passphrase, setPassphrase] = useState("");
  const [step1Errors, setStep1Errors] = useState<Record<string, string>>({});

  // Step 2 state
  const [verify, setVerify] = useState<VerifyState>({ kind: "idle" });
  const logRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [verify]);

  // Passphrase is only required for BloFin
  const requiresPassphrase = exchange === "blofin";

  function validateStep1(): boolean {
    const errors: Record<string, string> = {};
    if (!exchange) errors.exchange = "Select an exchange.";
    if (!apiKey.trim()) errors.apiKey = "API key is required.";
    if (!secretKey.trim()) errors.secretKey = "Secret key is required.";
    if (requiresPassphrase && !passphrase.trim()) {
      errors.passphrase = "BloFin requires a passphrase.";
    }
    setStep1Errors(errors);
    return Object.keys(errors).length === 0;
  }

  async function runVerify() {
    if (!exchange) return; // type-narrowing: impossible given step 1 validation
    setVerify({ kind: "checking", visibleLines: 0 });

    // Cosmetic log animation — independent of the actual POST
    const lineTimers: ReturnType<typeof setTimeout>[] = [];
    for (let i = 0; i < VERIFY_STEP_LOG_LINES.length; i++) {
      lineTimers.push(setTimeout(() => {
        setVerify(prev => prev.kind === "checking"
          ? { kind: "checking", visibleLines: Math.max(prev.visibleLines, i + 1) }
          : prev);
      }, (i + 1) * 500));
    }

    try {
      const result = await allocatorApi.storeExchangeKeys({
        exchange,
        label: label.trim() || undefined,
        api_key: apiKey.trim(),
        api_secret: secretKey.trim(),
        passphrase: requiresPassphrase ? passphrase.trim() : undefined,
      });
      lineTimers.forEach(clearTimeout);
      setVerify({ kind: "ok", result });
      // Auto-advance to step 3 so user sees live permissions
      setTimeout(() => setStep(3), 400);
    } catch (err) {
      lineTimers.forEach(clearTimeout);
      const { status, detail } = parseApiError(err);
      console.error("storeExchangeKeys failed:", err);
      setVerify({ kind: "err", status, detail });
    }
  }

  const step1Valid =
    !!exchange
    && apiKey.trim().length > 0
    && secretKey.trim().length > 0
    && (!requiresPassphrase || passphrase.trim().length > 0);

  return (
    <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 5, padding: "20px 22px", marginTop: 14 }}>
      <div style={{ marginBottom: 14 }}>
        <span style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>Link a new exchange</span>
      </div>

      <StepBar current={step} />

      {step === 1 && (
        <div>
          {/* Exchange dropdown */}
          <div style={{ marginBottom: 10 }}>
            <div style={fieldLabelStyle}>Exchange</div>
            <select
              value={exchange}
              onChange={e => setExchange(e.target.value as "" | ExchangeSlug)}
              style={{ ...inputStyle, appearance: "none", paddingRight: 28, cursor: "pointer" }}
            >
              <option value="">— Select —</option>
              {SUPPORTED_EXCHANGES.map(slug => (
                <option key={slug} value={slug}>{slug}</option>
              ))}
            </select>
            {step1Errors.exchange && <div style={inlineErrorStyle}>{step1Errors.exchange}</div>}
          </div>

          {/* Exchange-specific guidance */}
          {exchange === "binance" && (
            <div style={{
              fontSize: 10, color: "var(--t2)", lineHeight: 1.6,
              background: "var(--bg2)", border: "1px solid var(--line)",
              borderRadius: 3, padding: "10px 12px", marginBottom: 12,
            }}>
              <div style={{ fontSize: 9, fontWeight: 700, color: "var(--t3)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 6 }}>
                Create a Binance API key with:
              </div>
              <div>&middot; <span style={{ color: "var(--t1)" }}>Enable Reading</span></div>
              <div>&middot; <span style={{ color: "var(--t1)" }}>Enable Spot &amp; Margin Trading</span></div>
              <div>&middot; Do NOT enable <span style={{ color: "var(--red)" }}>Withdrawals</span></div>
            </div>
          )}
          {exchange === "blofin" && (
            <div style={{
              fontSize: 10, color: "var(--t2)", lineHeight: 1.6,
              background: "var(--bg2)", border: "1px solid var(--line)",
              borderRadius: 3, padding: "10px 12px", marginBottom: 12,
            }}>
              <div style={{ fontSize: 9, fontWeight: 700, color: "var(--t3)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 6 }}>
                Create a BloFin API key with:
              </div>
              <div>&middot; <span style={{ color: "var(--t1)" }}>Read + Trade</span> permissions</div>
              <div>&middot; Do NOT enable <span style={{ color: "var(--red)" }}>Transfer</span> or <span style={{ color: "var(--red)" }}>Withdraw</span></div>
            </div>
          )}

          {/* Label */}
          <div style={{ marginBottom: 10 }}>
            <div style={fieldLabelStyle}>Label (optional)</div>
            <input
              type="text"
              placeholder="e.g. Main account"
              value={label}
              onChange={e => setLabel(e.target.value)}
              style={inputStyle}
              onFocus={e => (e.target.style.borderColor = "var(--green)")}
              onBlur={e => (e.target.style.borderColor = "var(--line)")}
            />
          </div>

          {/* API key */}
          <div style={{ marginBottom: 10 }}>
            <div style={fieldLabelStyle}>API Key</div>
            <input
              type="text"
              placeholder="Your API key"
              value={apiKey}
              onChange={e => setApiKey(e.target.value)}
              style={inputStyle}
              onFocus={e => (e.target.style.borderColor = "var(--green)")}
              onBlur={e => (e.target.style.borderColor = "var(--line)")}
            />
            {step1Errors.apiKey && <div style={inlineErrorStyle}>{step1Errors.apiKey}</div>}
          </div>

          {/* Secret */}
          <div style={{ marginBottom: 10 }}>
            <div style={fieldLabelStyle}>Secret Key</div>
            <input
              type="password"
              placeholder="Your secret key"
              value={secretKey}
              onChange={e => setSecretKey(e.target.value)}
              style={inputStyle}
              onFocus={e => (e.target.style.borderColor = "var(--green)")}
              onBlur={e => (e.target.style.borderColor = "var(--line)")}
            />
            {step1Errors.secretKey && <div style={inlineErrorStyle}>{step1Errors.secretKey}</div>}
          </div>

          {/* Conditional passphrase for BloFin */}
          {requiresPassphrase && (
            <div style={{ marginBottom: 10 }}>
              <div style={fieldLabelStyle}>Passphrase</div>
              <input
                type="password"
                placeholder="BloFin passphrase"
                value={passphrase}
                onChange={e => setPassphrase(e.target.value)}
                style={inputStyle}
                onFocus={e => (e.target.style.borderColor = "var(--green)")}
                onBlur={e => (e.target.style.borderColor = "var(--line)")}
              />
              <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
                BloFin requires the passphrase you set when creating the API key.
              </div>
              {step1Errors.passphrase && <div style={inlineErrorStyle}>{step1Errors.passphrase}</div>}
            </div>
          )}

          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 10 }}>
            <button onClick={onCancel}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={textBtnStyle}>{"\u2190"} Back</button>
            <button
              onClick={() => { if (validateStep1()) setStep(2); }}
              disabled={!step1Valid}
              style={step1Valid ? primaryBtnStyle : disabledBtnStyle}
            >
              NEXT &rarr;
            </button>
          </div>
        </div>
      )}

      {step === 2 && (
        <div>
          {verify.kind !== "err" && (
            <div ref={logRef} style={{
              background: "var(--bg0)", border: "1px solid var(--line)", borderRadius: 3,
              padding: 12, height: 140, overflowY: "auto", marginBottom: 14, fontSize: 10, lineHeight: 1.9,
            }}>
              {verify.kind === "idle" && <span style={{ color: "var(--t2)" }}>_ awaiting verification</span>}
              {verify.kind === "checking" && (
                <>
                  {VERIFY_STEP_LOG_LINES.slice(0, verify.visibleLines).map((l, i) => (
                    <div key={i} style={{ color: "var(--t2)" }}>{l}</div>
                  ))}
                  <span style={{ color: "var(--t2)", animation: "blink-cursor 1s step-end infinite" }}>{"\u258C"}</span>
                </>
              )}
              {verify.kind === "ok" && (
                <>
                  {VERIFY_STEP_LOG_LINES.map((l, i) => (
                    <div key={i} style={{ color: "var(--t2)" }}>{l}</div>
                  ))}
                  <div style={{ color: "var(--green)" }}>{"> Connection verified \u2713"}</div>
                </>
              )}
            </div>
          )}

          {verify.kind === "err" && (
            <div style={{
              background: "var(--red-dim)", border: "1px solid var(--red)", borderRadius: 4,
              padding: "14px 16px", marginBottom: 14,
            }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: "var(--red)", marginBottom: 8 }}>
                {verify.status === 503 ? "\u2717 Exchange unreachable"
                  : verify.status === 400 ? "\u2717 Connection failed"
                  : "\u2717 Something went wrong"}
              </div>
              {verify.status === 503 && (
                <div style={{ fontSize: 10, color: "var(--t1)", marginBottom: 6 }}>
                  This may be temporary. Try again in a minute.
                </div>
              )}
              <div style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.5 }}>
                {verify.detail}
              </div>
            </div>
          )}

          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button
              onClick={() => { setStep(1); setVerify({ kind: "idle" }); }}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={textBtnStyle}
            >
              {"\u2190"} Back
            </button>
            <div>
              {verify.kind === "idle" && (
                <button onClick={runVerify} style={primaryBtnStyle}>RUN VERIFICATION</button>
              )}
              {verify.kind === "checking" && (
                <span style={{ fontSize: 10, color: "var(--t2)" }}>Verifying...</span>
              )}
              {verify.kind === "err" && (
                <button onClick={runVerify} style={primaryBtnStyle}>TRY AGAIN &rarr;</button>
              )}
            </div>
          </div>
        </div>
      )}

      {step === 3 && (
        <div>
          <PermissionsStep
            exchange={verify.kind === "ok" ? verify.result.exchange : (exchange || "")}
            permissions={verify.kind === "ok" ? verify.result.permissions : null}
          />
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button onClick={() => setStep(2)}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={textBtnStyle}>{"\u2190"} Back</button>
            {/* Step 3 is informational — backend already accepted the key on step 2.
                Gate only on having a permissions payload (defensive for malformed responses). */}
            {verify.kind === "ok" && verify.result.permissions ? (
              <button onClick={() => setStep(4)} style={primaryBtnStyle}>CONTINUE &rarr;</button>
            ) : (
              <button disabled style={disabledBtnStyle}>MISSING PERMISSIONS DATA</button>
            )}
          </div>
        </div>
      )}

      {step === 4 && verify.kind === "ok" && (
        <div style={{ padding: "16px 0" }}>
          <div style={{ textAlign: "center", marginBottom: 14 }}>
            <div style={{ fontSize: 48, marginBottom: 10, color: "var(--green)" }}>{"\u2713"}</div>
            <div style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)", marginBottom: 6 }}>
              {verify.result.exchange} connected
            </div>
            <div style={{ fontSize: 10, color: "var(--t2)" }}>Trading enabled. Withdrawals disabled.</div>
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button onClick={() => setStep(3)}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
              style={textBtnStyle}>{"\u2190"} Back</button>
            <button onClick={() => onComplete(verify.result.connection_id)} style={primaryBtnStyle}>
              DONE
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

// ─── Status pill (shared between cards + summary table) ─────────────────────

type PillTheme = { label: string; color: string; bg: string; border: string };

function statusTheme(status: Exchange["status"]): PillTheme {
  switch (status) {
    case "active":
      return { label: "CONNECTED", color: "var(--green)", bg: "var(--green-dim)", border: "var(--green-mid)" };
    case "pending_validation":
      return { label: "VALIDATING", color: "var(--amber)", bg: "var(--amber-dim)", border: "var(--amber)" };
    case "invalid":
      return { label: "INVALID", color: "var(--red)", bg: "var(--red-dim)", border: "var(--red)" };
    case "errored":
      return { label: "ERROR", color: "var(--amber)", bg: "var(--amber-dim)", border: "var(--amber)" };
    case "revoked":
      return { label: "REVOKED", color: "var(--t3)", bg: "var(--bg2)", border: "var(--line)" };
  }
}

function StatusPill({ status, size = "md" }: { status: Exchange["status"]; size?: "sm" | "md" }) {
  const t = statusTheme(status);
  return (
    <span style={{
      fontSize: size === "sm" ? 8 : 9,
      fontWeight: 700, letterSpacing: "0.12em",
      padding: size === "sm" ? "2px 6px" : "3px 8px",
      borderRadius: 3,
      background: t.bg, color: t.color, border: `1px solid ${t.border}`,
      textTransform: "uppercase",
    }}>
      {t.label}
    </span>
  );
}

export default function SettingsPage() {
  const router = useRouter();
  const { exchanges, instances, removeExchange, loading, refresh } = useTrader();
  const [showWizard, setShowWizard] = useState(false);
  const [confirmRemove, setConfirmRemove] = useState<string | null>(null);
  const [blockedRemove, setBlockedRemove] = useState<string | null>(null);

  async function handleWizardComplete() {
    setShowWizard(false);
    setConfirmRemove(null);
    // Trigger a refresh so the new connection appears in the list with status=active.
    // Snapshot was already fetched inline by the backend; the next /snapshots call picks it up.
    try { await allocatorApi.refreshSnapshots(); } catch { /* non-fatal */ }
    await refresh();
  }

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
                <StatusPill status={ex.status} size="sm" />
                <span style={{ fontSize: 9, color: "var(--t2)" }}>{ex.maskedKey}</span>
              </div>
            ) : (
              /* Full expanded format */
              <div key={ex.id} style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 18px" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <span style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>{ex.name}</span>
                    <StatusPill status={ex.status} />
                  </div>
                  {confirmRemove === ex.id ? (
                    <div style={{ display: "flex", gap: 6 }}>
                      <button onClick={async () => {
                        try { await allocatorApi.removeExchange(ex.id); } catch { /* soft-delete may fail if already removed */ }
                        removeExchange(ex.id); setConfirmRemove(null);
                        refresh();
                      }}
                        style={{ background: "var(--red)", color: "var(--bg0)", border: "none", borderRadius: 3, padding: "4px 10px", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", cursor: "pointer" }}>CONFIRM</button>
                      <button onClick={() => setConfirmRemove(null)}
                        style={{ background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, padding: "4px 10px", fontSize: 9, cursor: "pointer" }}>CANCEL</button>
                    </div>
                  ) : (
                    <button onClick={() => {
                      const liveOnExchange = instances.filter(i => i.exchangeId === ex.id && i.status === "live");
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

                {/* Status-specific metadata line */}
                {ex.status === "active" && (
                  <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t1)" }}>
                    <span>{ex.maskedKey}</span>
                    <span style={{ color: "var(--t2)" }}>read-only</span>
                    <span style={{ color: "var(--t2)" }}>synced {ex.lastSynced}</span>
                    {ex.balance > 0 && (
                      <span style={{ color: "var(--t1)" }}>
                        ${ex.balance.toLocaleString("en-US", { maximumFractionDigits: 2 })}
                      </span>
                    )}
                  </div>
                )}

                {ex.status === "pending_validation" && (
                  <>
                    <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t2)" }}>
                      <span>{ex.maskedKey}</span>
                    </div>
                    <div style={{ fontSize: 10, color: "var(--amber)", marginTop: 4, lineHeight: 1.5 }}>
                      Awaiting first permissions check. This usually resolves within 5 minutes.
                    </div>
                  </>
                )}

                {ex.status === "invalid" && (
                  <>
                    <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t2)" }}>
                      <span>{ex.maskedKey}</span>
                    </div>
                    {ex.lastErrorMsg && (
                      <div style={{ fontSize: 10, color: "var(--red)", marginTop: 4, lineHeight: 1.5, opacity: 0.9 }}>
                        {ex.lastErrorMsg}
                      </div>
                    )}
                  </>
                )}

                {ex.status === "errored" && (
                  <>
                    <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t2)" }}>
                      <span>{ex.maskedKey}</span>
                    </div>
                    {ex.lastErrorMsg && (
                      <div style={{ fontSize: 10, color: "var(--amber)", marginTop: 4, lineHeight: 1.5, opacity: 0.9 }}>
                        {ex.lastErrorMsg}
                      </div>
                    )}
                    <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
                      We&rsquo;ll retry automatically in a few minutes.
                    </div>
                  </>
                )}

                {blockedRemove === ex.id && (() => {
                  const liveOnExchange = instances.filter(i => i.exchangeId === ex.id && i.status === "live");
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
            {loading && (
              <div style={{ padding: "24px 0", textAlign: "center", color: "var(--t2)", fontSize: 10 }}>Loading exchanges...</div>
            )}
            {exchanges.length === 0 && !showWizard && !loading && (
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
          <LinkWizard onComplete={handleWizardComplete} onCancel={() => setShowWizard(false)} />
        )}

        {/* Exchange accounts summary table */}
        {exchanges.length > 0 && !showWizard && (
          <div style={{ marginTop: 20 }}>
            <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginBottom: 12 }}>
              ACCOUNT SUMMARY
            </div>
            <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6, overflow: "hidden" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid var(--line)" }}>
                    {["EXCHANGE", "BALANCE", "ALLOCATED", "TRADERS", "STATUS"].map(h => (
                      <th key={h} style={{ padding: "7px 14px", textAlign: "left", fontSize: 9, color: "var(--t3)", fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {exchanges.map((ex, idx) => {
                    const isActive = ex.status === "active";
                    const exInstances = instances.filter(i => i.exchangeId === ex.id && i.status === "live");
                    const exAllocated = exInstances.reduce((s, i) => s + (i.allocation ?? 0), 0);
                    const pctDeployed = ex.balance > 0 ? Math.min(100, (exAllocated / ex.balance) * 100).toFixed(1) : "0.0";
                    const dotColor = statusTheme(ex.status).color;
                    return (
                      <tr key={ex.id} style={{ borderBottom: idx < exchanges.length - 1 ? "1px solid var(--line)" : "none" }}>
                        <td style={{ padding: "10px 14px" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <span style={{ width: 5, height: 5, borderRadius: "50%", background: dotColor, flexShrink: 0 }} />
                            <span style={{ fontSize: 10, fontWeight: 700, color: "var(--t0)" }}>{ex.name}</span>
                          </div>
                        </td>
                        <td style={{ padding: "10px 14px" }}>
                          <div style={{ fontSize: 10, color: isActive ? "var(--t1)" : "var(--t3)" }}>
                            {isActive ? `$${ex.balance.toLocaleString("en-US")}` : "—"}
                          </div>
                        </td>
                        <td style={{ padding: "10px 14px" }}>
                          <div style={{ fontSize: 10, color: isActive ? "var(--t1)" : "var(--t3)" }}>
                            {isActive ? `$${exAllocated.toLocaleString("en-US")}` : "—"}
                          </div>
                          {isActive && (
                            <div style={{ fontSize: 9, color: "var(--t3)" }}>{exInstances.length} traders &middot; {pctDeployed}%</div>
                          )}
                        </td>
                        <td style={{ padding: "10px 14px", fontSize: 10, color: isActive ? "var(--t1)" : "var(--t3)" }}>
                          {isActive ? exInstances.length : "—"}
                        </td>
                        <td style={{ padding: "10px 14px" }}>
                          <StatusPill status={ex.status} />
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

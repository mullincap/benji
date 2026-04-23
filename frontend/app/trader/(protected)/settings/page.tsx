"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useRouter } from "next/navigation";
import { useTrader, Exchange } from "../../context";
import {
  allocatorApi,
  parseApiError,
  type ExchangeSlug,
  type ExchangePermissions,
  type StoreKeysSuccess,
  type ApiCapitalEvent,
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
  // Our BloFin parser encodes readOnly=0 as spot_trade=true + futures_trade=true.
  // By policy we require trading on BloFin, so render these as ACTIVE when the
  // backend confirms trade capability.
  { key: "spot_trade",    label: "Spot & margin trading", binance: "required", blofin: "required" },
  { key: "futures_trade", label: "Futures trading",       binance: "allowed",  blofin: "required" },
  // Withdrawals row intentionally omitted — backend still enforces withdrawals=false
  // via validate_permissions, and the UI guidance text on step 1 already tells
  // users to leave it disabled. Rendering it here added noise (amber "UNCLEAR"
  // on BloFin which can't distinguish it) without providing actionable info.
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
              <div>&middot; Do not enable <span style={{ color: "var(--t1)" }}>Withdrawals</span></div>
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
              <div>&middot; Do not enable <span style={{ color: "var(--t1)" }}>Transfer</span> or <span style={{ color: "var(--t1)" }}>Withdraw</span></div>
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

function ConfirmRemoveModal({
  exchangeName, submitting, onCancel, onConfirm,
}: {
  exchangeName: string;
  submitting: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={() => { if (!submitting) onCancel(); }}
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background: "var(--bg2)", border: "1px solid var(--line2)",
          borderRadius: 6, padding: "20px 24px",
          width: 480, maxWidth: "92vw",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 10,
        }}>
          Remove linked exchange?
        </div>
        <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
          Remove the linked <span style={{ color: "var(--t0)" }}>{exchangeName}</span> exchange?
          Any traders using this connection will stop.
        </div>
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button
            type="button"
            disabled={submitting}
            onClick={onCancel}
            style={{
              background: "transparent", border: "1px solid var(--line2)",
              borderRadius: 4, padding: "8px 16px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase", color: "var(--t2)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={submitting}
            onClick={onConfirm}
            style={{
              background: "var(--red-dim)", border: "1px solid var(--red)",
              borderRadius: 4, padding: "8px 16px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase", color: "var(--red)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            Remove
          </button>
        </div>
      </div>
    </div>
  );
}

// ─── Capital Events section ─────────────────────────────────────────────────
//
// Operator-recorded out-of-band capital movements. Subtracted from PnL math
// in /pnl so trading returns aren't conflated with principal moves. Today
// these are entered by hand; an exchange income API auto-importer is
// deferred (see docs/open_work_list.md "Capital change tracking").

const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";

function fmtCapitalDate(iso: string | null): string {
  if (!iso) return "—";
  // YYYY-MM-DD HH:MM (UTC), trim seconds
  const d = new Date(iso);
  const pad = (n: number) => n.toString().padStart(2, "0");
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} `
    + `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`
  );
}

function fmtAmount(usd: number, kind: "deposit" | "withdrawal"): string {
  const sign = kind === "deposit" ? "+" : "−";
  return `${sign}$${usd.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

// ─── Per-exchange capital-events panel ─────────────────────────────────────
//
// Renders one exchange connection: the allocations hanging off it (with
// principal summaries + Edit-anchor link) followed by a table of the
// events physically tied to that connection. Each row has a Credit-To
// dropdown scoped to THAT connection's allocations — you can't credit a
// BloFin deposit to a Binance allocation, and vice versa.

function ExchangeCapitalGroup({
  group,
  pnlByAlloc,
  onEditAnchor,
  onUpdateEvent,
  onDeleteEvent,
  onRecordNew,
  onReset,
}: {
  group: {
    connection_id: string | null;
    exchange_name: string | null;
    allocations: { id: string; label: string }[];
    events: ApiCapitalEvent[];
  };
  pnlByAlloc: Record<string, {
    principal_usd: number;
    principal_baseline_usd: number;
    principal_anchor_at: string | null;
    principal_anchor_explicit: boolean;
    principal_baseline_explicit: boolean;
    net_since_anchor_usd: number;
  }>;
  onEditAnchor: (allocationId: string) => void;
  onUpdateEvent: (ev: ApiCapitalEvent) => void;
  onDeleteEvent: (ev: ApiCapitalEvent) => void;
  // Optional — only present on real exchange panels (not the orphan "no
  // exchange link" bucket, where these actions don't apply).
  onRecordNew?: () => void;
  onReset?: () => void;
}) {

  const header = group.exchange_name
    ? group.exchange_name.toUpperCase()
    : "UNASSIGNED (no exchange link)";

  return (
    <div style={{
      background: "var(--bg1)", border: "1px solid var(--line)",
      borderRadius: 6, overflow: "hidden",
    }}>
      {/* Exchange header with inline RESET + RECORD buttons */}
      <div style={{
        padding: "10px 14px",
        borderBottom: "1px solid var(--line)",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        gap: 12,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12, minWidth: 0 }}>
          <span style={{
            fontSize: 11, fontWeight: 700, color: "var(--t0)",
            letterSpacing: "0.05em",
          }}>{header}</span>
          <span style={{
            fontSize: 9, color: "var(--t3)",
            letterSpacing: "0.12em", textTransform: "uppercase",
          }}>
            {group.allocations.length} alloc · {group.events.length} event{group.events.length === 1 ? "" : "s"}
          </span>
        </div>
        {(onReset || onRecordNew) && (
          <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
            {onReset && (
              <button
                onClick={onReset}
                disabled={group.allocations.length === 0 && group.events.length === 0}
                title={`Delete manual entries + overrides on ${group.exchange_name ?? "this exchange"} and re-sync from the exchange.`}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 3, color: "var(--amber)",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", padding: "5px 12px",
                  cursor: "pointer", fontFamily: FONT_MONO,
                }}
              >RESET TO DEFAULT</button>
            )}
            {onRecordNew && (
              <button
                onClick={onRecordNew}
                disabled={group.allocations.length === 0}
                title={group.allocations.length === 0
                  ? "No allocations on this exchange yet"
                  : `Record a capital event on ${group.exchange_name ?? "this exchange"}`}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 3,
                  color: group.allocations.length === 0 ? "var(--t3)" : "var(--green)",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", padding: "5px 12px",
                  cursor: group.allocations.length === 0 ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >+ RECORD CAPITAL EVENT</button>
            )}
          </div>
        )}
      </div>

      {/* Per-allocation principal summary (none = no tracking for this exchange yet) */}
      {group.allocations.length > 0 ? (
        <div style={{
          padding: "10px 14px",
          borderBottom: "1px solid var(--line)",
          display: "flex", flexDirection: "column", gap: 6,
          background: "var(--bg2)",
        }}>
          {group.allocations.map(opt => {
            const p = pnlByAlloc[opt.id];
            return (
              <div key={opt.id} style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                fontSize: 10, fontFamily: FONT_MONO,
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 14, flex: 1 }}>
                  <span style={{ color: "var(--t1)", fontWeight: 700, minWidth: 180 }}>
                    {opt.label}
                  </span>
                  {p ? (
                    <>
                      <span style={{ color: "var(--t3)" }}>PRINCIPAL</span>
                      <span style={{ color: "var(--t0)", fontWeight: 700 }}>
                        ${p.principal_usd.toLocaleString("en-US", { maximumFractionDigits: 2 })}
                      </span>
                      <span style={{ color: "var(--t3)" }}>
                        = ${p.principal_baseline_usd.toLocaleString("en-US", { maximumFractionDigits: 2 })}
                        {" "}
                        {p.net_since_anchor_usd >= 0 ? "+" : "−"} $
                        {Math.abs(p.net_since_anchor_usd).toLocaleString("en-US", { maximumFractionDigits: 2 })}
                      </span>
                      <span style={{ color: "var(--t3)" }}>
                        since {p.principal_anchor_at ? p.principal_anchor_at.slice(0, 10) : "—"}
                        {!p.principal_anchor_explicit && !p.principal_baseline_explicit ? " (default)" : ""}
                      </span>
                    </>
                  ) : (
                    <span style={{ color: "var(--t3)" }}>Loading…</span>
                  )}
                </div>
                <button
                  onClick={() => onEditAnchor(opt.id)}
                  style={{
                    background: "transparent", border: "none",
                    color: "var(--t2)", fontSize: 9,
                    letterSpacing: "0.12em", textTransform: "uppercase",
                    cursor: "pointer", fontFamily: FONT_MONO,
                  }}
                  onMouseEnter={e => (e.currentTarget.style.color = "var(--t0)")}
                  onMouseLeave={e => (e.currentTarget.style.color = "var(--t2)")}
                >
                  Edit anchor
                </button>
              </div>
            );
          })}
        </div>
      ) : group.connection_id ? (
        <div style={{
          padding: "10px 14px",
          borderBottom: "1px solid var(--line)",
          fontSize: 10, color: "var(--t3)", fontStyle: "italic",
          background: "var(--bg2)",
        }}>
          No active allocations on this exchange — events recorded but not counted toward any principal.
        </div>
      ) : null}

      {/* Events table */}
      {group.events.length === 0 ? (
        <div style={{ padding: "14px", fontSize: 10, color: "var(--t3)" }}>
          No capital events recorded for this exchange yet.
        </div>
      ) : (
        <table style={{
          width: "100%", borderCollapse: "collapse", fontSize: 10,
          fontFamily: FONT_MONO, tableLayout: "fixed",
        }}>
          <colgroup>
            <col style={{ width: "18%" }} />  {/* date */}
            <col style={{ width: "10%" }} />  {/* kind */}
            <col style={{ width: "12%" }} />  {/* amount */}
            <col />                             {/* notes */}
            <col style={{ width: "150px" }} />{/* actions */}
          </colgroup>
          <thead>
            <tr style={{ borderBottom: "1px solid var(--line)" }}>
              {["DATE (UTC)", "KIND", "AMOUNT", "NOTES", ""].map(h => (
                <th key={h} style={{
                  padding: "7px 14px", textAlign: "left",
                  fontSize: 9, color: "var(--t3)", fontWeight: 700,
                  letterSpacing: "0.12em", textTransform: "uppercase",
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {group.events.map((ev, idx) => {
              const amountColor = ev.kind === "deposit" ? "var(--green)" : "var(--amber)";
              let sourceBg = "var(--bg2)", sourceColor = "var(--t2)", sourceLabel = "MANUAL";
              if (ev.source === "auto") {
                sourceBg = "var(--green-dim)"; sourceColor = "var(--green)"; sourceLabel = "AUTO";
              } else if (ev.source === "auto-anomaly") {
                sourceBg = "var(--amber-dim)"; sourceColor = "var(--amber)"; sourceLabel = "ANOMALY";
              }
              if (ev.is_manually_overridden && ev.source !== "manual") {
                sourceLabel += "*";
              }
              return (
                <tr key={ev.event_id} style={{
                  borderBottom: idx < group.events.length - 1 ? "1px solid var(--line)" : "none",
                }}>
                  <td style={{ padding: "10px 14px", color: "var(--t1)" }}>
                    {fmtCapitalDate(ev.event_at)}
                    <span
                      title={
                        ev.source === "manual"
                          ? "Operator-entered"
                          : ev.source === "auto-anomaly"
                            ? "Auto-detected via mid-session equity-jump anomaly"
                            : "Auto-detected from exchange income API"
                          + (ev.is_manually_overridden ? " — edited by operator (won't re-sync)" : "")
                      }
                      style={{
                        marginLeft: 8,
                        display: "inline-block",
                        fontSize: 8, fontWeight: 700, letterSpacing: "0.08em",
                        padding: "1px 5px", borderRadius: 2,
                        background: sourceBg, color: sourceColor,
                        cursor: "help", verticalAlign: "middle",
                      }}
                    >
                      {sourceLabel}
                    </span>
                  </td>
                  <td style={{
                    padding: "10px 14px", color: "var(--t2)",
                    textTransform: "uppercase",
                  }}>{ev.kind}</td>
                  <td style={{
                    padding: "10px 14px", color: amountColor, fontWeight: 700,
                  }}>{fmtAmount(ev.amount_usd, ev.kind)}</td>
                  <td
                    title={ev.notes ?? ""}
                    style={{
                      padding: "10px 14px", color: "var(--t2)",
                      overflow: "hidden", textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >{ev.notes ?? "—"}</td>
                  <td style={{
                    padding: "10px 14px", textAlign: "right",
                    whiteSpace: "nowrap",
                  }}>
                    <button
                      onClick={() => onUpdateEvent(ev)}
                      style={{
                        background: "transparent", border: "none",
                        color: "var(--t1)", fontSize: 9, fontWeight: 700,
                        letterSpacing: "0.12em", textTransform: "uppercase",
                        marginRight: 12, cursor: "pointer",
                        fontFamily: FONT_MONO,
                      }}
                      onMouseEnter={e => (e.currentTarget.style.color = "var(--green)")}
                      onMouseLeave={e => (e.currentTarget.style.color = "var(--t1)")}
                    >Update</button>
                    <button
                      onClick={() => onDeleteEvent(ev)}
                      style={{
                        background: "transparent", border: "none",
                        color: "var(--t3)", fontSize: 9, fontWeight: 700,
                        letterSpacing: "0.12em", textTransform: "uppercase",
                        cursor: "pointer", fontFamily: FONT_MONO,
                      }}
                      onMouseEnter={e => (e.currentTarget.style.color = "var(--red)")}
                      onMouseLeave={e => (e.currentTarget.style.color = "var(--t3)")}
                    >Delete</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}


function CapitalEventModal({
  initial,
  allocations,
  submitting,
  onCancel,
  onSubmit,
}: {
  initial: ApiCapitalEvent | null; // null = create, set = edit
  allocations: { id: string; label: string }[];
  submitting: boolean;
  onCancel: () => void;
  onSubmit: (data: {
    allocation_id: string;
    amount_usd: number;
    kind: "deposit" | "withdrawal";
    event_at?: string;
    notes?: string;
  }) => void;
}) {
  // Default event_at to "now" formatted as YYYY-MM-DDTHH:MM (datetime-local).
  const nowLocal = (() => {
    const d = new Date();
    const pad = (n: number) => n.toString().padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
      + `T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  })();

  const [allocationId, setAllocationId] = useState(
    initial?.allocation_id ?? (allocations[0]?.id ?? ""),
  );
  const [kind, setKind] = useState<"deposit" | "withdrawal">(
    initial?.kind ?? "deposit",
  );
  const [amount, setAmount] = useState<string>(
    initial ? initial.amount_usd.toString() : "",
  );
  // The HTML datetime-local input wants local-time format. Initial values come
  // in as UTC ISO; convert by trimming the timezone suffix.
  const [eventAt, setEventAt] = useState<string>(
    initial
      ? initial.event_at.slice(0, 16)
      : nowLocal,
  );
  const [notes, setNotes] = useState<string>(initial?.notes ?? "");
  const [errorText, setErrorText] = useState<string | null>(null);

  const valid = allocationId
    && parseFloat(amount) > 0
    && (kind === "deposit" || kind === "withdrawal");

  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={() => { if (!submitting) onCancel(); }}
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background: "var(--bg2)", border: "1px solid var(--line2)",
          borderRadius: 6, padding: "20px 24px",
          width: 480, maxWidth: "92vw",
          fontFamily: FONT_MONO,
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
        }}>
          {initial ? "Edit capital event" : "Record capital event"}
        </div>

        <div style={{ marginBottom: 10 }}>
          <div style={fieldLabelStyle}>Allocation</div>
          <select
            value={allocationId}
            onChange={e => setAllocationId(e.target.value)}
            disabled={!!initial}  // immutable on edit (would orphan the row)
            style={{ ...inputStyle, appearance: "none", paddingRight: 28, cursor: initial ? "not-allowed" : "pointer" }}
          >
            {allocations.length === 0 && <option value="">— No allocations —</option>}
            {allocations.map(a => (
              <option key={a.id} value={a.id}>{a.label}</option>
            ))}
          </select>
        </div>

        <div style={{ display: "flex", gap: 10, marginBottom: 10 }}>
          <div style={{ flex: 1 }}>
            <div style={fieldLabelStyle}>Kind</div>
            <select
              value={kind}
              onChange={e => setKind(e.target.value as "deposit" | "withdrawal")}
              style={{ ...inputStyle, appearance: "none", paddingRight: 28, cursor: "pointer" }}
            >
              <option value="deposit">Deposit</option>
              <option value="withdrawal">Withdrawal</option>
            </select>
          </div>
          <div style={{ flex: 1 }}>
            <div style={fieldLabelStyle}>Amount (USD)</div>
            <input
              type="number"
              min="0"
              step="0.01"
              placeholder="0.00"
              value={amount}
              onChange={e => setAmount(e.target.value)}
              style={inputStyle}
              onFocus={e => (e.target.style.borderColor = "var(--green)")}
              onBlur={e => (e.target.style.borderColor = "var(--line)")}
            />
          </div>
        </div>

        <div style={{ marginBottom: 10 }}>
          <div style={fieldLabelStyle}>Event time (local)</div>
          <input
            type="datetime-local"
            value={eventAt}
            onChange={e => setEventAt(e.target.value)}
            style={inputStyle}
            onFocus={e => (e.target.style.borderColor = "var(--green)")}
            onBlur={e => (e.target.style.borderColor = "var(--line)")}
          />
        </div>

        <div style={{ marginBottom: 14 }}>
          <div style={fieldLabelStyle}>Notes (optional)</div>
          <input
            type="text"
            placeholder="e.g. Binance → BloFin transfer"
            value={notes}
            onChange={e => setNotes(e.target.value)}
            style={inputStyle}
            onFocus={e => (e.target.style.borderColor = "var(--green)")}
            onBlur={e => (e.target.style.borderColor = "var(--line)")}
          />
        </div>

        {errorText && (
          <div style={{ fontSize: 10, color: "var(--red)", marginBottom: 10 }}>
            {errorText}
          </div>
        )}

        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button
            type="button"
            disabled={submitting}
            onClick={onCancel}
            style={{
              background: "transparent", border: "1px solid var(--line2)",
              borderRadius: 4, padding: "8px 16px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase", color: "var(--t2)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: FONT_MONO,
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={!valid || submitting}
            onClick={() => {
              try {
                // datetime-local has no timezone — interpret as local, convert to UTC ISO.
                const eventAtIso = eventAt
                  ? new Date(eventAt).toISOString()
                  : undefined;
                onSubmit({
                  allocation_id: allocationId,
                  amount_usd: parseFloat(amount),
                  kind,
                  event_at: eventAtIso,
                  notes: notes.trim() || undefined,
                });
              } catch (e) {
                setErrorText(e instanceof Error ? e.message : String(e));
              }
            }}
            style={(!valid || submitting) ? disabledBtnStyle : primaryBtnStyle}
          >
            {submitting ? "Saving…" : (initial ? "Save" : "Record")}
          </button>
        </div>
      </div>
    </div>
  );
}

function CapitalEventsSection() {
  const { instances, exchanges } = useTrader();
  const [events, setEvents] = useState<ApiCapitalEvent[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editingEvent, setEditingEvent] = useState<ApiCapitalEvent | null>(null);
  // When recording a new event, track which connection initiated so the
  // modal can scope the allocation dropdown + default-set connection_id.
  // null = modal closed; string = connection_id of the exchange panel
  // whose "+ RECORD" button was clicked.
  const [creatingForConnection, setCreatingForConnection] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<ApiCapitalEvent | null>(null);
  // confirmReset holds the connection_id being reset; null = modal closed.
  const [confirmReset, setConfirmReset] = useState<string | null>(null);
  const [editingAnchor, setEditingAnchor] = useState<string | null>(null);
  // Per-allocation principal cache (allocation_id → ApiPnl). Fed by /pnl.
  const [pnlByAlloc, setPnlByAlloc] = useState<Record<string, {
    principal_usd: number;
    principal_baseline_usd: number;
    principal_anchor_at: string | null;
    principal_anchor_explicit: boolean;
    principal_baseline_explicit: boolean;
    net_since_anchor_usd: number;
  }>>({});

  const refresh = useCallback(async () => {
    try {
      const r = await allocatorApi.getCapitalEvents();
      setEvents(r.events);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  const refreshPnl = useCallback(async (allocIds: string[]) => {
    const out: typeof pnlByAlloc = {};
    await Promise.all(allocIds.map(async id => {
      try {
        const r = await allocatorApi.getPnl(id);
        out[id] = {
          principal_usd: r.principal_usd,
          principal_baseline_usd: r.principal_baseline_usd,
          principal_anchor_at: r.principal_anchor_at,
          principal_anchor_explicit: r.principal_anchor_explicit,
          principal_baseline_explicit: r.principal_baseline_explicit,
          net_since_anchor_usd: r.net_since_anchor_usd,
        };
      } catch {
        // non-fatal; alloc simply won't render a principal summary
      }
    }));
    setPnlByAlloc(out);
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  // Build allocation dropdown options from useTrader.instances. Use
  // strategyName + exchangeName as the user-readable label; id is the
  // allocation_id the backend expects.
  const allocOptions = instances
    .filter(i => i.id && !i.id.startsWith("temp-"))
    .map(i => ({
      id: i.id,
      label: `${i.strategyName}${i.exchangeName ? ` · ${i.exchangeName}` : ""}`,
    }));

  const allocLabelById: Record<string, string> = {};
  for (const opt of allocOptions) allocLabelById[opt.id] = opt.label;

  // Fetch principal summary for each allocation after the picker materializes.
  // Refreshes on events-list change so edits propagate automatically.
  useEffect(() => {
    if (allocOptions.length > 0) refreshPnl(allocOptions.map(a => a.id));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instances, events]);

  async function handleCreate(data: Parameters<typeof allocatorApi.createCapitalEvent>[0]) {
    setSubmitting(true);
    try {
      await allocatorApi.createCapitalEvent(data);
      setCreatingForConnection(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleEdit(eventId: string, data: Parameters<typeof allocatorApi.updateCapitalEvent>[1]) {
    setSubmitting(true);
    try {
      await allocatorApi.updateCapitalEvent(eventId, data);
      setEditingEvent(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete(eventId: string) {
    setSubmitting(true);
    try {
      await allocatorApi.deleteCapitalEvent(eventId);
      setConfirmDelete(null);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleReset(connectionId: string) {
    setSubmitting(true);
    try {
      await allocatorApi.resetCapitalEventsToDefaults(connectionId);
      setConfirmReset(null);
      await refresh();
      // Silent success — the list refresh is the visual confirmation.
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleAnchorSave(
    allocationId: string,
    data: { anchor_at?: string; baseline_usd?: number; reset?: boolean },
  ) {
    setSubmitting(true);
    try {
      await allocatorApi.updateAllocation(allocationId, data.reset ? {
        clear_principal_anchor: true,
        clear_principal_baseline: true,
      } : {
        principal_anchor_at: data.anchor_at,
        principal_baseline_usd: data.baseline_usd,
      });
      setEditingAnchor(null);
      await refreshPnl(allocOptions.map(a => a.id));
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div style={{ marginTop: 20 }}>
      <div style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
      }}>
        CAPITAL EVENTS
      </div>

      {error && (
        <div style={{
          background: "var(--red-dim)", border: "1px solid var(--red)",
          borderRadius: 4, padding: "8px 12px", marginBottom: 8,
          fontSize: 10, color: "var(--red)",
        }}>
          {error}
        </div>
      )}

      {/* Exchange-first grouping. Each exchange connection gets its own
          panel with: the allocations hanging off it (principal summary +
          Edit anchor), then the events physically tied to that connection
          (CREDIT TO dropdown lets operator reassign to any allocation on
          the same connection). Events without a connection_id (legacy
          manual entries) fall into a "No exchange" bucket at the end. */}
      {events === null ? (
        <div style={{
          background: "var(--bg1)", border: "1px solid var(--line)",
          borderRadius: 6, padding: "16px", fontSize: 10, color: "var(--t2)",
        }}>
          Loading…
        </div>
      ) : (
        (() => {
          // Build connection-first grouping.
          type ConnectionGroup = {
            connection_id: string | null;
            exchange_name: string | null;  // null if the group has no events linked to a connection
            allocations: typeof allocOptions;
            events: ApiCapitalEvent[];
          };
          const groups: ConnectionGroup[] = [];
          const byConn: Record<string, ConnectionGroup> = {};
          // Seed from exchanges so we always show a panel per linked exchange,
          // even with zero events. Maps Exchange.id → connection_id (same in
          // our system — Exchange.id is the connection UUID).
          for (const ex of exchanges) {
            const key = ex.id;
            const group: ConnectionGroup = {
              connection_id: key,
              exchange_name: ex.name || ex.exchange,
              allocations: instances
                .filter(i => i.id && !i.id.startsWith("temp-") && i.connectionId === key)
                .map(i => ({
                  id: i.id,
                  label: `${i.strategyName}${i.exchangeName ? ` · ${i.exchangeName}` : ""}`,
                })),
              events: [],
            };
            byConn[key] = group;
            groups.push(group);
          }
          const orphan: ConnectionGroup = {
            connection_id: null,
            exchange_name: null,
            allocations: [],
            events: [],
          };
          for (const ev of events) {
            if (ev.connection_id && byConn[ev.connection_id]) {
              byConn[ev.connection_id].events.push(ev);
            } else {
              orphan.events.push(ev);
            }
          }
          if (orphan.events.length > 0) groups.push(orphan);

          return (
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {groups.map(grp => (
                <ExchangeCapitalGroup
                  key={grp.connection_id || "orphan"}
                  group={grp}
                  pnlByAlloc={pnlByAlloc}
                  onEditAnchor={setEditingAnchor}
                  onUpdateEvent={setEditingEvent}
                  onDeleteEvent={setConfirmDelete}
                  onRecordNew={grp.connection_id ? () => setCreatingForConnection(grp.connection_id!) : undefined}
                  onReset={grp.connection_id ? () => setConfirmReset(grp.connection_id!) : undefined}
                />
              ))}
              {groups.length === 0 && (
                <div style={{
                  background: "var(--bg1)", border: "1px solid var(--line)",
                  borderRadius: 6, padding: "16px", fontSize: 10, color: "var(--t2)",
                }}>
                  No linked exchanges yet. Capital events appear once you link an exchange above.
                </div>
              )}
            </div>
          );
        })()
      )}

      {(creatingForConnection || editingEvent) && (() => {
        // On CREATE: scope the allocation dropdown to just those on the
        // initiating exchange's connection. On EDIT: show all the
        // operator's allocations (remap path for manual overrides).
        const modalAllocations = creatingForConnection
          ? instances
              .filter(i =>
                i.id && !i.id.startsWith("temp-")
                && i.connectionId === creatingForConnection,
              )
              .map(i => ({
                id: i.id,
                label: `${i.strategyName}${i.exchangeName ? ` · ${i.exchangeName}` : ""}`,
              }))
          : allocOptions;
        return (
          <CapitalEventModal
            initial={editingEvent}
            allocations={modalAllocations}
            submitting={submitting}
            onCancel={() => { setCreatingForConnection(null); setEditingEvent(null); }}
            onSubmit={data => {
              if (editingEvent) {
                handleEdit(editingEvent.event_id, {
                  amount_usd: data.amount_usd,
                  kind: data.kind,
                  event_at: data.event_at,
                  notes: data.notes,
                });
              } else {
                handleCreate({
                  ...data,
                  // Tie new events to the exchange panel they were initiated from
                  connection_id: creatingForConnection ?? undefined,
                });
              }
            }}
          />
        );
      })()}

      {confirmDelete && (
        <div
          role="dialog"
          aria-modal="true"
          onClick={() => { if (!submitting) setConfirmDelete(null); }}
          style={{
            position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              background: "var(--bg2)", border: "1px solid var(--line2)",
              borderRadius: 6, padding: "20px 24px",
              width: 420, maxWidth: "92vw", fontFamily: FONT_MONO,
            }}
          >
            <div style={{
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              color: "var(--t3)", textTransform: "uppercase", marginBottom: 10,
            }}>Delete capital event?</div>
            <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
              {fmtAmount(confirmDelete.amount_usd, confirmDelete.kind)} on {fmtCapitalDate(confirmDelete.event_at)}.
              {" "}This will be removed from PnL reconciliation immediately.
            </div>
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button
                type="button"
                disabled={submitting}
                onClick={() => setConfirmDelete(null)}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--t2)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >Cancel</button>
              <button
                type="button"
                disabled={submitting}
                onClick={() => handleDelete(confirmDelete.event_id)}
                style={{
                  background: "var(--red-dim)", border: "1px solid var(--red)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--red)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >Delete</button>
            </div>
          </div>
        </div>
      )}

      {/* Reset-to-default confirm (connection-scoped) */}
      {confirmReset && (() => {
        const exchangeName = exchanges.find(e => e.id === confirmReset)?.name
          ?? exchanges.find(e => e.id === confirmReset)?.exchange
          ?? confirmReset.slice(0, 8);
        return (
        <div
          role="dialog"
          aria-modal="true"
          onClick={() => { if (!submitting) setConfirmReset(null); }}
          style={{
            position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            onClick={e => e.stopPropagation()}
            style={{
              background: "var(--bg2)", border: "1px solid var(--line2)",
              borderRadius: 6, padding: "20px 24px",
              width: 520, maxWidth: "92vw", fontFamily: FONT_MONO,
            }}
          >
            <div style={{
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              color: "var(--t3)", textTransform: "uppercase", marginBottom: 10,
            }}>Reset {exchangeName} capital events?</div>
            <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
              Delete all capital events on <span style={{ color: "var(--t0)" }}>{exchangeName}</span> —
              manual entries AND manual overrides on auto-detected rows — then re-sync from
              the exchange. Only events the exchange reports will remain. Principal on this
              exchange's allocations recomputes from exchange-truth values. Other exchanges
              are not affected. This cannot be undone.
            </div>
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <button
                type="button"
                disabled={submitting}
                onClick={() => setConfirmReset(null)}
                style={{
                  background: "transparent", border: "1px solid var(--line2)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--t2)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >Cancel</button>
              <button
                type="button"
                disabled={submitting}
                onClick={() => handleReset(confirmReset)}
                style={{
                  background: "var(--amber-dim)", border: "1px solid var(--amber)",
                  borderRadius: 4, padding: "8px 16px",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                  textTransform: "uppercase", color: "var(--amber)",
                  cursor: submitting ? "not-allowed" : "pointer",
                  fontFamily: FONT_MONO,
                }}
              >{submitting ? "Resetting…" : "Reset"}</button>
            </div>
          </div>
        </div>
        );
      })()}

      {/* Anchor-edit modal */}
      {editingAnchor && (() => {
        const existing = pnlByAlloc[editingAnchor];
        const allocLabel = allocLabelById[editingAnchor] || editingAnchor.slice(0, 8);
        return (
          <AnchorEditModal
            allocationLabel={allocLabel}
            current={existing ?? null}
            submitting={submitting}
            onCancel={() => setEditingAnchor(null)}
            onSave={data => handleAnchorSave(editingAnchor, data)}
          />
        );
      })()}
    </div>
  );
}

function AnchorEditModal({
  allocationLabel,
  current,
  submitting,
  onCancel,
  onSave,
}: {
  allocationLabel: string;
  current: {
    principal_usd: number;
    principal_baseline_usd: number;
    principal_anchor_at: string | null;
    principal_anchor_explicit: boolean;
    principal_baseline_explicit: boolean;
  } | null;
  submitting: boolean;
  onCancel: () => void;
  onSave: (data: { anchor_at?: string; baseline_usd?: number; reset?: boolean }) => void;
}) {
  const [anchorAt, setAnchorAt] = useState<string>(
    current?.principal_anchor_at
      ? current.principal_anchor_at.slice(0, 16)
      : "",
  );
  const [baseline, setBaseline] = useState<string>(
    current ? current.principal_baseline_usd.toString() : "",
  );

  const hasOverride = !!current && (
    current.principal_anchor_explicit || current.principal_baseline_explicit
  );

  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={() => { if (!submitting) onCancel(); }}
      style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background: "var(--bg2)", border: "1px solid var(--line2)",
          borderRadius: 6, padding: "20px 24px",
          width: 480, maxWidth: "92vw", fontFamily: FONT_MONO,
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 12,
        }}>
          Principal anchor — {allocationLabel}
        </div>

        <div style={{ fontSize: 10, color: "var(--t2)", marginBottom: 14, lineHeight: 1.5 }}>
          Pin the start date of the tracked track record. Events before this date are
          excluded from principal + session-history calculations. Leaving both blank
          falls back to the allocation's creation date and initial capital.
        </div>

        <div style={{ marginBottom: 10 }}>
          <div style={fieldLabelStyle}>Anchor date/time (local)</div>
          <input
            type="datetime-local"
            value={anchorAt}
            onChange={e => setAnchorAt(e.target.value)}
            style={inputStyle}
            onFocus={e => (e.target.style.borderColor = "var(--green)")}
            onBlur={e => (e.target.style.borderColor = "var(--line)")}
          />
        </div>

        <div style={{ marginBottom: 14 }}>
          <div style={fieldLabelStyle}>Baseline USD at anchor</div>
          <input
            type="number"
            min="0"
            step="0.01"
            placeholder="e.g. 5000.00"
            value={baseline}
            onChange={e => setBaseline(e.target.value)}
            style={inputStyle}
            onFocus={e => (e.target.style.borderColor = "var(--green)")}
            onBlur={e => (e.target.style.borderColor = "var(--line)")}
          />
        </div>

        <div style={{ display: "flex", gap: 8, justifyContent: "space-between" }}>
          <button
            type="button"
            disabled={submitting || !hasOverride}
            onClick={() => onSave({ reset: true })}
            title={hasOverride
              ? "Clear the override and fall back to default (allocation created_at + capital_usd)"
              : "No override to clear"}
            style={{
              background: "transparent", border: "1px solid var(--line2)",
              borderRadius: 4, padding: "8px 14px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: !hasOverride ? "var(--t3)" : "var(--amber)",
              cursor: (!hasOverride || submitting) ? "not-allowed" : "pointer",
              fontFamily: FONT_MONO,
            }}
          >Use default</button>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              disabled={submitting}
              onClick={onCancel}
              style={{
                background: "transparent", border: "1px solid var(--line2)",
                borderRadius: 4, padding: "8px 16px",
                fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
                textTransform: "uppercase", color: "var(--t2)",
                cursor: submitting ? "not-allowed" : "pointer",
                fontFamily: FONT_MONO,
              }}
            >Cancel</button>
            <button
              type="button"
              disabled={submitting || !anchorAt || !(parseFloat(baseline) > 0)}
              onClick={() => {
                try {
                  const iso = anchorAt ? new Date(anchorAt).toISOString() : undefined;
                  onSave({
                    anchor_at: iso,
                    baseline_usd: parseFloat(baseline),
                  });
                } catch { /* noop */ }
              }}
              style={
                (submitting || !anchorAt || !(parseFloat(baseline) > 0))
                  ? disabledBtnStyle : primaryBtnStyle
              }
            >{submitting ? "Saving…" : "Save"}</button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function SettingsPage() {
  const router = useRouter();
  const { exchanges, instances, removeExchange, loading, refresh } = useTrader();
  const [showWizard, setShowWizard] = useState(false);
  const [removingId, setRemovingId] = useState<string | null>(null);
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
                  <button
                    onClick={async () => {
                      const liveOnExchange = instances.filter(i => i.exchangeId === ex.id && i.status === "live");
                      if (liveOnExchange.length > 0) {
                        setBlockedRemove(ex.id);
                        setConfirmRemove(null);
                        return;
                      }
                      if (ex.status === "active") {
                        // Non-destructive but in-use — require confirmation modal.
                        setConfirmRemove(ex.id);
                        setBlockedRemove(null);
                        return;
                      }
                      // Non-active row (errored / invalid / pending_validation) — single click, no confirm.
                      setRemovingId(ex.id);
                      try {
                        await allocatorApi.removeExchange(ex.id);
                      } catch (err) {
                        console.error("removeExchange failed:", err);
                      } finally {
                        setRemovingId(null);
                      }
                      removeExchange(ex.id);
                      refresh();
                    }}
                    disabled={removingId === ex.id}
                    onMouseEnter={e => { e.currentTarget.style.color = "var(--red)"; }}
                    onMouseLeave={e => { e.currentTarget.style.color = "var(--t3)"; }}
                    style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", cursor: removingId === ex.id ? "not-allowed" : "pointer", transition: "color 0.15s ease", opacity: removingId === ex.id ? 0.5 : 1 }}
                  >
                    {removingId === ex.id ? "REMOVING…" : "REMOVE"}
                  </button>
                </div>

                {/* Status-specific metadata line */}
                {ex.status === "active" && (
                  <div style={{ display: "flex", gap: 16, fontSize: 10, color: "var(--t1)" }}>
                    <span>{ex.maskedKey}</span>
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

        {/* Capital events — operator-recorded deposits/withdrawals.
            Always rendered (regardless of wizard state) since it's a separate
            concern from exchange linking. */}
        {!showWizard && <CapitalEventsSection />}
      </div>

      {confirmRemove && (() => {
        const target = exchanges.find(e => e.id === confirmRemove);
        if (!target) return null;
        return (
          <ConfirmRemoveModal
            exchangeName={target.name || target.exchange}
            submitting={removingId === target.id}
            onCancel={() => setConfirmRemove(null)}
            onConfirm={async () => {
              setRemovingId(target.id);
              try {
                await allocatorApi.removeExchange(target.id);
              } catch (err) {
                console.error("removeExchange failed:", err);
              } finally {
                setRemovingId(null);
                setConfirmRemove(null);
              }
              removeExchange(target.id);
              refresh();
            }}
          />
        );
      })()}
    </div>
  );
}

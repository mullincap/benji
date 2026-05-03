"use client";

/**
 * frontend/app/trader/components/ExchangeLinkWizard.tsx
 * ======================================================
 * Reusable 4-step wizard for linking a trader exchange account.
 * Used by:
 *   - /trader/settings        — "Link a new exchange" CTA
 *   - /trader/get-started     — inline onboarding hero (the BloFin /
 *                                Binance card click hands off to here
 *                                without leaving the page)
 *
 * Steps: Keys → Verify → Permissions → Done.
 *
 * Component contract:
 *   initialExchange? — pre-selects Step 1's dropdown. Pass the
 *     exchange slug the user picked on /get-started, or `null` (the
 *     default) for a fresh select. Settings passes null since it's
 *     opened directly via "Link a new exchange" without context.
 *   onSuccess — called with the new connection_id after the user
 *     clicks DONE on Step 4. Caller decides next action (route to
 *     /trader/overview, refresh the linked-exchanges list, etc.).
 *   onCancel — called when the user clicks Back on Step 1 (the only
 *     "leave the wizard" affordance). Caller decides next action
 *     (hide the wizard, return to a hero card, etc.).
 *
 * Layout: the component is intentionally layout-agnostic — no max-
 * width or external margins. Caller wraps for centering / sizing.
 *   Settings: rendered inline in its existing wizard slot.
 *   Get-started: wrapped in a <div style={{maxWidth:720,margin:'0 auto'}}>
 *                so it doesn't stretch wide on the chrome-light
 *                onboarding layout.
 *
 * Form styles: the field-label / input / button consts below are
 * intentionally a duplicate of the equivalents in
 * /trader/settings/page.tsx — that page also uses them for its
 * Capital Events add form and the Baseline Anchor edit modal.
 * Sharing them via a third module is correct long-term; for now
 * the duplication is small (~30 lines) and lets this extraction
 * stay a pure code-move.
 */

import { useEffect, useRef, useState } from "react";

import {
  allocatorApi,
  parseApiError,
  type ExchangeSlug,
  type ExchangePermissions,
  type StoreKeysSuccess,
} from "../api";
import { resolveExchangeInfo } from "../_lib/exchange-info";

const SUPPORTED_EXCHANGES: ExchangeSlug[] = ["binance", "blofin"];
const VERIFY_STEP_LOG_LINES = [
  "> Connecting to exchange API...",
  "> Verifying key ownership...",
  "> Checking permissions...",
];

// Display-cased names for the success step. Backend stores slugs lowercased
// ("blofin", "binance") which read as scrappy on the celebration screen.
const EXCHANGE_DISPLAY: Record<string, string> = {
  blofin: "BloFin",
  binance: "Binance",
};

// Truncate a UUID-shaped connection id for the receipt: first 8 + last 4
// with an ellipsis in the middle. Same shape Stripe / GitHub use for
// short-form ids in confirmation surfaces.
function truncateConnectionId(id: string): string {
  if (id.length <= 14) return id;
  return id.slice(0, 8) + "…" + id.slice(-4);
}

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
              }}>{done ? "✓" : n}</div>
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

// ─── Form styling (duplicated with settings page — see file header) ─────────

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

// ─── Wizard ──────────────────────────────────────────────────────────────────

type VerifyState =
  | { kind: "idle" }
  | { kind: "checking"; visibleLines: number }
  | { kind: "ok"; result: StoreKeysSuccess }
  | { kind: "err"; status: number; detail: string };

export function ExchangeLinkWizard({
  initialExchange = null,
  onSuccess,
  onCancel,
}: {
  initialExchange?: ExchangeSlug | null;
  onSuccess: (connectionId: string) => void;
  onCancel: () => void;
}) {
  const [step, setStep] = useState<1 | 2 | 3 | 4>(1);

  // Step 1 form state
  const [exchange, setExchange] = useState<"" | ExchangeSlug>(initialExchange ?? "");
  const [apiKey, setApiKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [passphrase, setPassphrase] = useState("");
  const [step1Errors, setStep1Errors] = useState<Record<string, string>>({});
  // "Need help creating your API keys?" expandable. Collapsed by
  // default — experienced users skip it; new users have a one-click
  // path to per-exchange step-by-step guidance + the API-management
  // link.
  const [showApiKeyHelp, setShowApiKeyHelp] = useState(false);

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
          {/* Exchange dropdown — only shown when the caller didn't pre-select.
              Get-started always pre-selects via initialExchange (the user
              already clicked BloFin / Binance on the hero). Settings's
              "+ LINK A NEW EXCHANGE" path passes null, so the dropdown
              shows there for fresh selection. */}
          {!initialExchange && (
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
          )}

          {/* Exchange-specific API-key creation guidance — only shown when
              the caller didn't pre-select. With a pre-selection the user
              has already passed through a surface (e.g. the get-started
              hero's trust bullets) that established the permission story;
              repeating it inside Step 1 was redundant. The guidance still
              appears in settings's fresh-select path because that surface
              has no upstream context. */}
          {!initialExchange && exchange === "binance" && (
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
          {!initialExchange && exchange === "blofin" && (
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

          {/* "Need help creating your API keys?" expandable.
              Renders only when an exchange is selected (so we can
              show the right per-exchange instructions) AND that
              exchange has a non-empty instruction set in
              EXCHANGE_INFO. Silently degrades to nothing for
              future exchanges added before their entry lands. */}
          {(() => {
            if (!exchange) return null;
            const info = resolveExchangeInfo(exchange);
            if (info.apiKeyInstructions.length === 0) return null;
            return (
              <div style={{ marginBottom: 12 }}>
                <button
                  type="button"
                  onClick={() => setShowApiKeyHelp(v => !v)}
                  style={{
                    display: "inline-flex", alignItems: "center", gap: 6,
                    padding: "4px 0",
                    background: "transparent",
                    border: "none",
                    color: "var(--allocator)",
                    fontSize: 11,
                    fontWeight: 700,
                    fontFamily: "inherit",
                    cursor: "pointer",
                  }}
                >
                  Need help creating your API keys?
                  <span style={{
                    display: "inline-block",
                    transition: "transform 0.15s ease",
                    transform: showApiKeyHelp ? "rotate(180deg)" : "rotate(0deg)",
                    fontSize: 9,
                  }}>▾</span>
                </button>
                {showApiKeyHelp && (
                  <div style={{
                    marginTop: 8,
                    padding: "12px 14px",
                    background: "var(--bg2)",
                    border: "1px solid var(--line)",
                    borderLeft: "3px solid var(--allocator)",
                    borderRadius: 3,
                  }}>
                    <ol style={{
                      margin: "0 0 12px",
                      paddingLeft: 18,
                      color: "var(--t1)",
                      fontSize: 11,
                      lineHeight: 1.7,
                    }}>
                      {info.apiKeyInstructions.map((line, i) => (
                        <li key={i}>{line}</li>
                      ))}
                    </ol>
                    {info.apiKeyManagementUrl !== "#" && (
                      <a
                        href={info.apiKeyManagementUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{
                          display: "inline-flex", alignItems: "center", gap: 6,
                          padding: "7px 12px",
                          background: "var(--allocator)",
                          color: "#0d0518",
                          border: "1px solid var(--allocator)",
                          borderRadius: 2,
                          fontSize: 9,
                          fontWeight: 700,
                          letterSpacing: "0.14em",
                          textTransform: "uppercase",
                          textDecoration: "none",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {info.apiKeyInstructionsLabel}
                      </a>
                    )}
                  </div>
                )}
              </div>
            );
          })()}

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
              style={textBtnStyle}>{"←"} Back</button>
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
                  <span style={{ color: "var(--t2)", animation: "blink-cursor 1s step-end infinite" }}>{"▌"}</span>
                </>
              )}
              {verify.kind === "ok" && (
                <>
                  {VERIFY_STEP_LOG_LINES.map((l, i) => (
                    <div key={i} style={{ color: "var(--t2)" }}>{l}</div>
                  ))}
                  <div style={{ color: "var(--green)" }}>{"> Connection verified ✓"}</div>
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
                {verify.status === 503 ? "✗ Exchange unreachable"
                  : verify.status === 400 ? "✗ Connection failed"
                  : "✗ Something went wrong"}
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
              {"←"} Back
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
              style={textBtnStyle}>{"←"} Back</button>
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

      {step === 4 && verify.kind === "ok" && (() => {
        const exchangeName = EXCHANGE_DISPLAY[verify.result.exchange] ?? verify.result.exchange;
        const detailRows: { label: string; value: string; mono?: boolean; tone?: "green" }[] = [
          { label: "Connection ID", value: truncateConnectionId(verify.result.connection_id), mono: true },
          { label: "Permissions",   value: verify.result.exchange === "blofin" ? "Read · Trade" : "Read · Spot · Margin" },
          { label: "Withdrawals",   value: "Disabled", tone: "green" },
          { label: "Encryption",    value: "Fernet · at-rest" },
        ];
        return (
          <div style={{ padding: "8px 0 4px" }}>
            {/* Hero check inside a green ring — celebration moment, sized to
                feel distinct from the small step-bar checks above it. */}
            <div style={{ display: "flex", justifyContent: "center", marginBottom: 22 }}>
              <div style={{
                width: 76, height: 76, borderRadius: "50%",
                background: "var(--green-dim)",
                border: "2px solid var(--green)",
                display: "flex", alignItems: "center", justifyContent: "center",
                boxShadow: "0 0 0 6px rgba(0, 200, 150, 0.05)",
              }}>
                <svg width="36" height="36" viewBox="0 0 36 36" fill="none">
                  <path d="M9 18 L15.5 24.5 L27 12" stroke="var(--green)" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </div>
            </div>

            {/* Heading */}
            <div style={{ textAlign: "center", marginBottom: 22 }}>
              <div style={{ fontSize: 18, fontWeight: 700, color: "var(--t0)", marginBottom: 5, letterSpacing: "-0.01em" }}>
                {exchangeName} connected
              </div>
              <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", textTransform: "uppercase", fontWeight: 700 }}>
                Verified · just now
              </div>
            </div>

            {/* Receipt — concrete confirmation of what landed. Reads as
                financial-grade evidence, not just "we did the thing". */}
            <div style={{
              background: "var(--bg0)",
              border: "1px solid var(--line)",
              borderRadius: 3,
              padding: "0 16px",
              marginBottom: 22,
            }}>
              {detailRows.map((row, i) => (
                <div key={row.label} style={{
                  display: "flex", alignItems: "center", justifyContent: "space-between",
                  padding: "11px 0",
                  borderBottom: i < detailRows.length - 1 ? "1px solid var(--line)" : "none",
                }}>
                  <span style={{
                    fontSize: 9, color: "var(--t3)",
                    letterSpacing: "0.12em", textTransform: "uppercase", fontWeight: 700,
                  }}>
                    {row.label}
                  </span>
                  <span style={{
                    fontSize: 11,
                    color: row.tone === "green" ? "var(--green)" : "var(--t1)",
                    fontFamily: row.mono
                      ? "var(--font-space-mono), Space Mono, monospace"
                      : undefined,
                    letterSpacing: row.mono ? "0.04em" : undefined,
                  }}>
                    {row.value}
                  </span>
                </div>
              ))}
            </div>

            {/* Forward momentum line — answers the "now what?" question that
                a celebration screen otherwise leaves dangling. */}
            <div style={{
              textAlign: "center",
              fontSize: 10, color: "var(--t2)",
              marginBottom: 20,
              letterSpacing: "0.04em",
            }}>
              Next: pick a strategy to deploy your capital.
            </div>

            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <button onClick={() => setStep(3)}
                onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; }}
                onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; }}
                style={textBtnStyle}>{"←"} Back</button>
              <button onClick={() => onSuccess(verify.result.connection_id)} style={primaryBtnStyle}>
                DONE
              </button>
            </div>
          </div>
        );
      })()}
    </div>
  );
}

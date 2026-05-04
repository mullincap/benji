"use client";

import { useEffect, useMemo, useState } from "react";

import { changePassword } from "../admin/_lib/api";
import { useAuth } from "../lib/auth";
import { scorePassword } from "../auth/_components/PasswordStrengthMeter";
import {
  type AccountProfile,
  fetchAccount,
  updateProfile,
} from "./_api";

const ALLOCATOR_PURPLE = "#a78bff";
const ALLOCATOR_PURPLE_HOVER = "#c0a8ff";
const LABEL_WIDTH = 140;

// ─── Page ───────────────────────────────────────────────────────────────────

export default function AccountPage() {
  const { refetch: refetchAuth } = useAuth();
  const [profile, setProfile] = useState<AccountProfile | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchAccount()
      .then((p) => { if (!cancelled) setProfile(p); })
      .catch((err) => { if (!cancelled) setLoadError(err instanceof Error ? err.message : String(err)); });
    return () => { cancelled = true; };
  }, []);

  return (
    <main
      style={{
        flex: 1,
        padding: "64px 32px 96px",
        maxWidth: 720,
        width: "100%",
        margin: "0 auto",
      }}
    >
      <div style={{ marginBottom: 56 }}>
        <div style={{ color: "var(--t0)", fontSize: 22, fontWeight: 700, letterSpacing: "-0.01em", marginBottom: 6 }}>
          Account
        </div>
        <div style={{ color: "var(--t2)", fontSize: 13 }}>
          Manage your profile and security.
        </div>
      </div>

      {loadError && (
        <div style={{
          padding: "12px 16px",
          marginBottom: 32,
          border: "1px solid var(--red)",
          borderRadius: 2,
          color: "var(--red)",
          fontSize: 12,
        }}>
          Failed to load account: {loadError}
        </div>
      )}

      {profile && (
        <>
          <IdentitySection profile={profile} />
          <ProfileSection profile={profile} onSaved={async (p) => {
            setProfile(p);
            await refetchAuth().catch(() => {});
          }} />
          <PasswordSection isTemporary={profile.password_is_temporary} />
          <DangerZoneSection />
        </>
      )}
    </main>
  );
}

// ─── Section primitives ─────────────────────────────────────────────────────

function Section({ children, last }: { children: React.ReactNode; last?: boolean }) {
  return (
    <section
      style={{
        paddingBottom: last ? 0 : 40,
        marginBottom: last ? 0 : 40,
        borderBottom: last ? "0" : "1px solid var(--line)",
      }}
    >
      {children}
    </section>
  );
}

function SectionHeader({ title, description, danger }: {
  title: string;
  description: string;
  danger?: boolean;
}) {
  return (
    <>
      <div style={{
        color: danger ? "var(--red)" : "var(--t0)",
        fontSize: 13, fontWeight: 700,
        letterSpacing: "0.06em", textTransform: "uppercase",
        marginBottom: 4,
      }}>
        {title}
      </div>
      <div style={{ color: "var(--t3)", fontSize: 12, marginBottom: 24 }}>
        {description}
      </div>
    </>
  );
}

function FieldRow({ label, children, editable }: {
  label: string;
  children: React.ReactNode;
  editable?: boolean;
}) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: `${LABEL_WIDTH}px 1fr`,
      gap: 24,
      alignItems: editable ? "flex-start" : "center",
      padding: "12px 0",
    }}>
      <span style={{
        color: "var(--t3)",
        fontSize: 11,
        letterSpacing: "0.06em",
        textTransform: "uppercase",
        paddingTop: editable ? 10 : 0,
      }}>
        {label}
      </span>
      <div>{children}</div>
    </div>
  );
}

// ─── Identity (read-only) ───────────────────────────────────────────────────

const ROLE_DOT_COLOR: Record<string, string> = {
  trader: "var(--green)",
  allocator: ALLOCATOR_PURPLE,
  admin: "var(--amber)",
};

function IdentitySection({ profile }: { profile: AccountProfile }) {
  const roleKey = profile.is_admin ? "admin" : (profile.role || "").toLowerCase();
  const roleColor = ROLE_DOT_COLOR[roleKey] || "var(--t2)";
  const roleLabel = profile.is_admin
    ? "Admin"
    : (profile.role ? profile.role.charAt(0).toUpperCase() + profile.role.slice(1).toLowerCase() : "—");

  return (
    <Section>
      <SectionHeader title="Identity" description="Read-only account information." />

      <FieldRow label="Email">
        <span style={{ color: "var(--t0)", fontSize: 13 }}>{profile.email}</span>
        <div style={{ color: "var(--t3)", fontSize: 11, marginTop: 4 }}>
          Locked. Contact admin to change.
        </div>
      </FieldRow>

      <FieldRow label="Role">
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6, color: "var(--t0)", fontSize: 12 }}>
          <span style={{ width: 6, height: 6, borderRadius: "50%", background: roleColor }} />
          {roleLabel}
        </span>
      </FieldRow>

      <FieldRow label="Joined">
        <span style={{ color: "var(--t0)", fontSize: 13, fontVariantNumeric: "tabular-nums" }}>
          {formatDate(profile.created_at)}
        </span>
      </FieldRow>

      <FieldRow label="Last sign-in">
        <span style={{ color: "var(--t1)", fontSize: 13, fontVariantNumeric: "tabular-nums" }}>
          {formatDateTime(profile.last_login)}
        </span>
      </FieldRow>

      <FieldRow label="Invited by">
        <span style={{ color: "var(--t1)", fontSize: 13 }}>
          {profile.invited_by || "— · System bootstrap"}
        </span>
      </FieldRow>

      <FieldRow label="User ID">
        <span style={{ color: "var(--t3)", fontSize: 12, fontVariantNumeric: "tabular-nums" }}>
          {profile.user_id}
        </span>
      </FieldRow>
    </Section>
  );
}

// ─── Profile (editable) ─────────────────────────────────────────────────────

type SaveStatus = { kind: "idle" } | { kind: "saving" } | { kind: "saved"; at: number } | { kind: "error"; msg: string };

function ProfileSection({ profile, onSaved }: { profile: AccountProfile; onSaved: (p: AccountProfile) => void | Promise<void> }) {
  const [firstName, setFirstName] = useState(profile.first_name || "");
  const [lastName, setLastName] = useState(profile.last_name || "");
  const [firm, setFirm] = useState(profile.firm || "");
  const [status, setStatus] = useState<SaveStatus>({ kind: "idle" });

  const initial = useMemo(() => ({
    first_name: profile.first_name || "",
    last_name: profile.last_name || "",
    firm: profile.firm || "",
  }), [profile]);

  const dirty =
    firstName.trim() !== initial.first_name.trim() ||
    lastName.trim() !== initial.last_name.trim() ||
    firm.trim() !== initial.firm.trim();

  const canSave = dirty && firstName.trim().length > 0 && lastName.trim().length > 0 && status.kind !== "saving";

  function reset() {
    setFirstName(initial.first_name);
    setLastName(initial.last_name);
    setFirm(initial.firm);
    setStatus({ kind: "idle" });
  }

  async function save() {
    if (!canSave) return;
    setStatus({ kind: "saving" });
    try {
      const next = await updateProfile({
        first_name: firstName.trim(),
        last_name: lastName.trim(),
        firm: firm.trim() || null,
      });
      await onSaved(next);
      setStatus({ kind: "saved", at: Date.now() });
    } catch (err) {
      setStatus({ kind: "error", msg: err instanceof Error ? err.message : String(err) });
    }
  }

  return (
    <Section>
      <SectionHeader title="Profile" description="Edit your name and firm." />

      <FieldRow label="First name" editable>
        <TextInput value={firstName} onChange={setFirstName} />
      </FieldRow>
      <FieldRow label="Last name" editable>
        <TextInput value={lastName} onChange={setLastName} />
      </FieldRow>
      <FieldRow label="Firm" editable>
        <TextInput value={firm} onChange={setFirm} />
        <div style={{ color: "var(--t3)", fontSize: 11, marginTop: 4 }}>Optional</div>
      </FieldRow>

      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        marginTop: 24,
      }}>
        <SaveStatusLabel status={status} />
        <div style={{ display: "flex", gap: 10 }}>
          <GhostButton disabled={!dirty || status.kind === "saving"} onClick={reset}>Reset</GhostButton>
          <PrimaryButton disabled={!canSave} onClick={save}>
            {status.kind === "saving" ? "Saving…" : "Save"}
          </PrimaryButton>
        </div>
      </div>
    </Section>
  );
}

function SaveStatusLabel({ status }: { status: SaveStatus }) {
  if (status.kind === "saved") {
    return (
      <span style={{ color: "var(--green)", fontSize: 11, display: "inline-flex", alignItems: "center", gap: 6 }}>
        <span>✓</span> Saved
      </span>
    );
  }
  if (status.kind === "error") {
    return <span style={{ color: "var(--red)", fontSize: 11 }}>{status.msg}</span>;
  }
  return null;
}

// ─── Password ───────────────────────────────────────────────────────────────

type PwStatus = { kind: "idle" } | { kind: "saving" } | { kind: "success" } | { kind: "error"; msg: string; field?: "current" | "new" };

function PasswordSection({ isTemporary }: { isTemporary: boolean }) {
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirmPw, setConfirmPw] = useState("");
  const [showCurrent, setShowCurrent] = useState(false);
  const [showNext, setShowNext] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [status, setStatus] = useState<PwStatus>({ kind: "idle" });

  const strength = scorePassword(next);
  const matches = next.length > 0 && next === confirmPw;
  const canSubmit =
    status.kind !== "saving" &&
    strength.meetsMinimum &&
    matches &&
    (isTemporary || current.length > 0);

  // Auto-clear success state after 4s, returning to default fields cleared.
  useEffect(() => {
    if (status.kind !== "success") return;
    const t = setTimeout(() => setStatus({ kind: "idle" }), 4000);
    return () => clearTimeout(t);
  }, [status]);

  async function submit() {
    if (!canSubmit) return;
    setStatus({ kind: "saving" });
    try {
      await changePassword(next, isTemporary ? undefined : current);
      setCurrent("");
      setNext("");
      setConfirmPw("");
      setStatus({ kind: "success" });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("Current password incorrect") || msg.includes("Current password required")) {
        setStatus({ kind: "error", msg: "Current password is incorrect.", field: "current" });
      } else if (msg.toLowerCase().includes("must be different")) {
        setStatus({ kind: "error", msg: "New password must be different from current.", field: "new" });
      } else if (msg.toLowerCase().includes("password too weak")) {
        setStatus({ kind: "error", msg: "Password too weak. Use 12+ chars with mixed case, number, and symbol.", field: "new" });
      } else {
        setStatus({ kind: "error", msg });
      }
    }
  }

  return (
    <Section>
      <SectionHeader
        title="Password"
        description={isTemporary
          ? "You're using a temporary password. Set a new one below."
          : "Change your password. You'll stay signed in on this device."}
      />

      {!isTemporary && (
        <FieldRow label="Current" editable>
          <PasswordInput
            value={current}
            onChange={setCurrent}
            visible={showCurrent}
            onToggle={() => setShowCurrent((v) => !v)}
            placeholder="Enter current password"
            invalid={status.kind === "error" && status.field === "current"}
          />
        </FieldRow>
      )}

      <FieldRow label="New" editable>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <PasswordInput
            value={next}
            onChange={setNext}
            visible={showNext}
            onToggle={() => setShowNext((v) => !v)}
            placeholder="Min 12 characters"
            invalid={status.kind === "error" && status.field === "new"}
          />
          <PasswordInput
            value={confirmPw}
            onChange={setConfirmPw}
            visible={showConfirm}
            onToggle={() => setShowConfirm((v) => !v)}
            placeholder="Re-enter new password"
            invalid={confirmPw.length > 0 && !matches}
          />
        </div>
        <PasswordRules password={next} />
        {confirmPw.length > 0 && !matches && (
          <div style={{ color: "var(--red)", fontSize: 11, marginTop: 6 }}>
            Passwords do not match.
          </div>
        )}
      </FieldRow>

      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        marginTop: 24,
        gap: 16,
      }}>
        <div style={{ flex: 1, minHeight: 16 }}>
          {status.kind === "success" && (
            <span style={{ color: "var(--green)", fontSize: 11, display: "inline-flex", alignItems: "center", gap: 6 }}>
              <span>✓</span> Password changed
            </span>
          )}
          {status.kind === "error" && !status.field && (
            <span style={{ color: "var(--red)", fontSize: 11 }}>{status.msg}</span>
          )}
        </div>
        <PrimaryButton disabled={!canSubmit} onClick={submit}>
          {status.kind === "saving" ? "Changing…" : "Change password"}
        </PrimaryButton>
      </div>
    </Section>
  );
}

function PasswordRules({ password }: { password: string }) {
  const len = password.length >= 12;
  const mixed = /[a-z]/.test(password) && /[A-Z]/.test(password);
  const hasNum = /[0-9]/.test(password);
  const hasSym = /[^A-Za-z0-9]/.test(password);
  const rules: Array<{ label: string; met: boolean }> = [
    { label: "12+ chars", met: len },
    { label: "Mixed case", met: mixed },
    { label: "Number", met: hasNum },
    { label: "Symbol", met: hasSym },
  ];
  return (
    <div style={{ marginTop: 10, fontSize: 11, color: "var(--t3)", lineHeight: 1.6 }}>
      {rules.map((r, i) => (
        <span key={r.label} style={{
          display: "inline-block",
          marginRight: i === rules.length - 1 ? 0 : 16,
          color: r.met ? "var(--green)" : "var(--t3)",
        }}>
          <span style={{ marginRight: 4 }}>{r.met ? "✓" : "○"}</span>
          {r.label}
        </span>
      ))}
    </div>
  );
}

// ─── Danger zone (stubbed) ──────────────────────────────────────────────────

function DangerZoneSection() {
  return (
    <Section last>
      <SectionHeader
        title="Danger zone"
        description="Account deletion and data export coming soon."
        danger
      />
      <FieldRow label="Export data">
        <span style={{ color: "var(--t3)", fontSize: 12 }}>Coming soon</span>
      </FieldRow>
      <FieldRow label="Sign out everywhere">
        <span style={{ color: "var(--t3)", fontSize: 12 }}>Coming soon</span>
      </FieldRow>
      <FieldRow label="Delete account">
        <span style={{ color: "var(--t3)", fontSize: 12 }}>Coming soon</span>
      </FieldRow>
    </Section>
  );
}

// ─── Inputs + buttons ───────────────────────────────────────────────────────

function TextInput({ value, onChange, placeholder }: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <input
      type="text"
      value={value}
      placeholder={placeholder}
      onChange={(e) => onChange(e.currentTarget.value)}
      onFocus={(e) => {
        e.currentTarget.style.borderColor = "var(--green)";
        e.currentTarget.style.boxShadow = "0 0 0 3px var(--green-soft)";
      }}
      onBlur={(e) => {
        e.currentTarget.style.borderColor = "var(--line2)";
        e.currentTarget.style.boxShadow = "none";
      }}
      style={{
        width: "100%",
        background: "var(--bg0)",
        border: "1px solid var(--line2)",
        color: "var(--t0)",
        fontFamily: "inherit",
        fontSize: 13,
        padding: "10px 12px",
        borderRadius: 2,
        outline: "none",
        transition: "border-color 0.12s ease, box-shadow 0.12s ease",
      }}
    />
  );
}

function PasswordInput({ value, onChange, visible, onToggle, placeholder, invalid }: {
  value: string;
  onChange: (v: string) => void;
  visible: boolean;
  onToggle: () => void;
  placeholder?: string;
  invalid?: boolean;
}) {
  return (
    <div style={{ position: "relative" }}>
      <input
        type={visible ? "text" : "password"}
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.currentTarget.value)}
        onFocus={(e) => {
          if (invalid) return;
          e.currentTarget.style.borderColor = "var(--green)";
          e.currentTarget.style.boxShadow = "0 0 0 3px var(--green-soft)";
        }}
        onBlur={(e) => {
          e.currentTarget.style.borderColor = invalid ? "var(--red)" : "var(--line2)";
          e.currentTarget.style.boxShadow = "none";
        }}
        style={{
          width: "100%",
          background: "var(--bg0)",
          border: `1px solid ${invalid ? "var(--red)" : "var(--line2)"}`,
          color: "var(--t0)",
          fontFamily: "inherit",
          fontSize: 13,
          padding: "10px 36px 10px 12px",
          borderRadius: 2,
          outline: "none",
          transition: "border-color 0.12s ease, box-shadow 0.12s ease",
        }}
      />
      <button
        type="button"
        onClick={onToggle}
        aria-label={visible ? "Hide password" : "Show password"}
        style={{
          position: "absolute",
          right: 8,
          top: "50%",
          transform: "translateY(-50%)",
          background: "transparent",
          border: 0,
          color: "var(--t3)",
          cursor: "pointer",
          fontSize: 11,
          letterSpacing: "0.06em",
          textTransform: "uppercase",
          fontFamily: "inherit",
          padding: "4px 6px",
        }}
      >
        {visible ? "HIDE" : "SHOW"}
      </button>
    </div>
  );
}

function PrimaryButton({ children, disabled, onClick }: {
  children: React.ReactNode;
  disabled?: boolean;
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      onMouseEnter={(e) => {
        if (disabled) return;
        e.currentTarget.style.background = ALLOCATOR_PURPLE_HOVER;
        e.currentTarget.style.borderColor = ALLOCATOR_PURPLE_HOVER;
      }}
      onMouseLeave={(e) => {
        if (disabled) return;
        e.currentTarget.style.background = ALLOCATOR_PURPLE;
        e.currentTarget.style.borderColor = ALLOCATOR_PURPLE;
      }}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "9px 16px",
        fontFamily: "inherit",
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: "0.1em",
        textTransform: "uppercase",
        border: `1px solid ${disabled ? "var(--line2)" : ALLOCATOR_PURPLE}`,
        borderRadius: 2,
        background: disabled ? "transparent" : ALLOCATOR_PURPLE,
        color: disabled ? "var(--t3)" : "#0d0518",
        cursor: disabled ? "not-allowed" : "pointer",
        whiteSpace: "nowrap",
        transition: "all 0.12s ease",
      }}
    >
      {children}
    </button>
  );
}

function GhostButton({ children, disabled, onClick }: {
  children: React.ReactNode;
  disabled?: boolean;
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      onMouseEnter={(e) => {
        if (disabled) return;
        e.currentTarget.style.color = "var(--t0)";
        e.currentTarget.style.borderColor = "var(--t2)";
      }}
      onMouseLeave={(e) => {
        if (disabled) return;
        e.currentTarget.style.color = "var(--t1)";
        e.currentTarget.style.borderColor = "var(--line2)";
      }}
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "9px 16px",
        fontFamily: "inherit",
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: "0.1em",
        textTransform: "uppercase",
        border: "1px solid var(--line2)",
        borderRadius: 2,
        background: "transparent",
        color: disabled ? "var(--t3)" : "var(--t1)",
        cursor: disabled ? "not-allowed" : "pointer",
        whiteSpace: "nowrap",
        transition: "all 0.12s ease",
      }}
    >
      {children}
    </button>
  );
}

// ─── Format helpers ─────────────────────────────────────────────────────────

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toISOString().slice(0, 10);
  } catch {
    return "—";
  }
}

function formatDateTime(iso: string | null): string {
  if (!iso) return "Never";
  try {
    const d = new Date(iso);
    const date = d.toISOString().slice(0, 10);
    const time = d.toISOString().slice(11, 16);
    return `${date} ${time} UTC`;
  } catch {
    return "—";
  }
}

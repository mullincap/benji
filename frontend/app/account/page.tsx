"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { changePassword } from "../admin/_lib/api";
import { useAuth } from "../lib/auth";
import { scorePassword } from "../auth/_components/PasswordStrengthMeter";
import {
  type AccountProfile,
  fetchAccount,
  updateProfile,
} from "./_api";
import AccountSidebar, { type NavId } from "./_AccountSidebar";

const ALLOCATOR_PURPLE = "#a78bff";
const ALLOCATOR_PURPLE_HOVER = "#c0a8ff";
const LABEL_WIDTH = 140;
const TOPBAR_H = 46;
// Anchored sections add scroll-margin so the heading isn't hidden behind
// the sticky topbar when the user clicks a sidebar item or hits a deep link.
const SCROLL_MARGIN_TOP = TOPBAR_H + 16;

const NAV_TITLES: Record<NavId, string> = {
  identity: "Identity",
  profile: "Profile",
  security: "Security",
  "danger-zone": "Danger Zone",
};

const NAV_ORDER: NavId[] = ["identity", "profile", "security", "danger-zone"];

// ─── Page ───────────────────────────────────────────────────────────────────

export default function AccountPage() {
  const { refetch: refetchAuth } = useAuth();
  const [profile, setProfile] = useState<AccountProfile | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [active, setActive] = useState<NavId>("identity");

  useEffect(() => {
    let cancelled = false;
    fetchAccount()
      .then((p) => { if (!cancelled) setProfile(p); })
      .catch((err) => { if (!cancelled) setLoadError(err instanceof Error ? err.message : String(err)); });
    return () => { cancelled = true; };
  }, []);

  // Scroll-spy: keep the sidebar's active state in sync with what the
  // user is currently viewing. rootMargin pushes the trigger line down
  // so a section becomes "active" once its heading clears the topbar.
  useEffect(() => {
    if (!profile) return;
    const targets = NAV_ORDER
      .map((id) => document.getElementById(id))
      .filter((el): el is HTMLElement => el !== null);
    if (targets.length === 0) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((e) => e.isIntersecting)
          .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
        if (visible.length === 0) return;
        const id = visible[0].target.id as NavId;
        setActive(id);
        // Update the URL hash without triggering a scroll jump.
        if (typeof window !== "undefined" && window.history.replaceState) {
          const hash = `#${id}`;
          if (window.location.hash !== hash) {
            window.history.replaceState(null, "", hash);
          }
        }
      },
      {
        // Trigger once a section's top crosses ~80px below the topbar.
        // Bottom margin -50% means a section stops being "active" once
        // its bottom passes the viewport midpoint, handing off to the
        // next section as the user scrolls.
        rootMargin: `-${TOPBAR_H + 36}px 0px -50% 0px`,
        threshold: 0,
      },
    );
    targets.forEach((el) => observer.observe(el));
    return () => observer.disconnect();
  }, [profile]);

  // Honor #anchor on initial mount (e.g. /account#security deep-link).
  // Wait one tick after profile loads so sections exist in the DOM.
  useEffect(() => {
    if (!profile) return;
    const hash = typeof window !== "undefined" ? window.location.hash.slice(1) : "";
    if (!hash) return;
    if (!NAV_ORDER.includes(hash as NavId)) return;
    const el = document.getElementById(hash);
    if (!el) return;
    // Defer one frame so layout settles before scrolling.
    requestAnimationFrame(() => {
      el.scrollIntoView({ behavior: "instant" as ScrollBehavior, block: "start" });
    });
  }, [profile]);

  const handleNavigate = useCallback((id: NavId) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.scrollIntoView({ behavior: "smooth", block: "start" });
    if (typeof window !== "undefined" && window.history.replaceState) {
      window.history.replaceState(null, "", `#${id}`);
    }
    // Optimistic active update — IntersectionObserver follows shortly.
    setActive(id);
  }, []);

  return (
    <div style={{
      flex: 1,
      display: "flex",
      alignItems: "stretch",
      minHeight: 0,
    }}>
      <AccountSidebar active={active} onNavigate={handleNavigate} />

      <main style={{
        flex: 1,
        minWidth: 0,
        padding: "48px 48px 96px",
        maxWidth: 920,
        width: "100%",
      }}>
        <PageHeading active={active} />

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
            <Anchor id="identity">
              <IdentitySection profile={profile} />
            </Anchor>

            <Anchor id="profile">
              <ProfileSection profile={profile} onSaved={async (p) => {
                setProfile(p);
                await refetchAuth().catch(() => {});
              }} />
            </Anchor>

            <Anchor id="security">
              <PasswordSection isTemporary={profile.password_is_temporary} />
              <TwoFactorSection />
              <ActiveSessionsSection profile={profile} />
            </Anchor>

            <Anchor id="danger-zone" last>
              <DangerZoneSection />
            </Anchor>
          </>
        )}
      </main>
    </div>
  );
}

// ─── Anchor wrapper ─────────────────────────────────────────────────────────

function Anchor({ id, children, last }: {
  id: NavId;
  children: React.ReactNode;
  last?: boolean;
}) {
  return (
    <div
      id={id}
      style={{
        scrollMarginTop: SCROLL_MARGIN_TOP,
        // Each anchor block ends with a divider (except the last) so
        // multiple sub-sections within an anchor (Security has three)
        // are visually grouped without their own dividers competing.
        paddingBottom: last ? 0 : 40,
        marginBottom: last ? 0 : 40,
        borderBottom: last ? "0" : "1px solid var(--line)",
      }}
    >
      {children}
    </div>
  );
}

// ─── Page heading ───────────────────────────────────────────────────────────

function PageHeading({ active }: { active: NavId }) {
  const isDanger = active === "danger-zone";
  const eyebrowColor = isDanger ? "var(--red)" : ALLOCATOR_PURPLE;
  const eyebrow = `[ Account · ${NAV_TITLES[active]} ]`;
  return (
    <div style={{ marginBottom: 48 }}>
      <div style={{
        color: eyebrowColor,
        fontSize: 10,
        letterSpacing: "0.18em",
        textTransform: "uppercase",
        marginBottom: 8,
        fontWeight: 700,
        transition: "color 0.2s ease",
      }}>
        {eyebrow}
      </div>
      <div style={{
        color: "var(--t0)",
        fontSize: 26,
        fontWeight: 700,
        letterSpacing: "-0.01em",
        marginBottom: 6,
      }}>
        Account
      </div>
      <div style={{ color: "var(--t2)", fontSize: 13 }}>
        Manage your profile and security.
      </div>
    </div>
  );
}

// ─── Section primitives ─────────────────────────────────────────────────────

function Section({ children, lastInGroup }: { children: React.ReactNode; lastInGroup?: boolean }) {
  return (
    <section style={{
      paddingBottom: lastInGroup ? 0 : 32,
      marginBottom: lastInGroup ? 0 : 32,
      borderBottom: lastInGroup ? "0" : "1px solid var(--line)",
    }}>
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
    <Section lastInGroup>
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

  const dirtyCount =
    (firstName.trim() !== initial.first_name.trim() ? 1 : 0) +
    (lastName.trim() !== initial.last_name.trim() ? 1 : 0) +
    (firm.trim() !== initial.firm.trim() ? 1 : 0);
  const dirty = dirtyCount > 0;
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
    <Section lastInGroup>
      <SectionHeader title="Profile" description="Edit your name and firm." />

      <FieldRow label="First name" editable>
        <TextInput value={firstName} onChange={setFirstName} dirty={firstName.trim() !== initial.first_name.trim()} />
      </FieldRow>
      <FieldRow label="Last name" editable>
        <TextInput value={lastName} onChange={setLastName} dirty={lastName.trim() !== initial.last_name.trim()} />
      </FieldRow>
      <FieldRow label="Firm" editable>
        <TextInput value={firm} onChange={setFirm} dirty={firm.trim() !== initial.firm.trim()} />
        <div style={{ color: "var(--t3)", fontSize: 11, marginTop: 4 }}>Optional</div>
      </FieldRow>

      <div style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        marginTop: 24,
      }}>
        <SaveStatusLabel status={status} dirtyCount={dirtyCount} />
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

function SaveStatusLabel({ status, dirtyCount }: { status: SaveStatus; dirtyCount: number }) {
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
  if (dirtyCount > 0) {
    return (
      <span style={{ color: "var(--amber)", fontSize: 11 }}>
        ● {dirtyCount} unsaved change{dirtyCount === 1 ? "" : "s"}
      </span>
    );
  }
  return <span />;
}

// ─── Security: Password (existing functionality) ────────────────────────────

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

// ─── Security: 2FA stub ─────────────────────────────────────────────────────

function TwoFactorSection() {
  return (
    <Section>
      <SectionHeader
        title="Two-factor authentication"
        description="Add an extra layer of security to your account."
      />
      <FieldRow label="Status">
        <span style={{ color: "var(--t2)", fontSize: 13 }}>Not enabled</span>
        <div style={{ color: "var(--t3)", fontSize: 11, marginTop: 4 }}>
          Coming soon — TOTP via authenticator app
        </div>
      </FieldRow>
      <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 16 }}>
        <GhostButton disabled>Enable 2FA</GhostButton>
      </div>
    </Section>
  );
}

// ─── Security: Active sessions stub ─────────────────────────────────────────

function ActiveSessionsSection({ profile }: { profile: AccountProfile }) {
  // Placeholder: backend doesn't surface per-session metadata yet (only
  // user_mgmt.users.last_login). When a sessions endpoint lands, this
  // section can render the real list.
  const lastSignIn = formatDateTime(profile.last_login);
  return (
    <Section lastInGroup>
      <SectionHeader
        title="Active sessions"
        description="Devices signed into your account."
      />
      <FieldRow label="This device">
        <span style={{ color: "var(--t0)", fontSize: 13 }}>Active now</span>
        <div style={{ color: "var(--t3)", fontSize: 11, marginTop: 4 }}>
          Last sign-in: {lastSignIn}
        </div>
      </FieldRow>
      <FieldRow label="Other sessions">
        <span style={{ color: "var(--t2)", fontSize: 13 }}>
          Per-session list coming soon
        </span>
      </FieldRow>
    </Section>
  );
}

// ─── Danger zone ────────────────────────────────────────────────────────────

function DangerZoneSection() {
  const items = [
    {
      title: "Export account data",
      body: "Download a copy of your profile, allocations, and trade history as JSON.",
    },
    {
      title: "Sign out everywhere",
      body: "Sign out from all devices. Useful if you suspect unauthorized access.",
    },
    {
      title: "Delete account",
      body: "Permanently remove your account, allocations, and trading history. Cannot be undone.",
      danger: true,
    },
  ];

  return (
    <Section lastInGroup>
      <SectionHeader
        title="Danger Zone"
        description="Destructive operations. These actions cannot be undone."
        danger
      />
      {items.map((item, i) => (
        <div key={item.title} style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "16px 0",
          borderBottom: i === items.length - 1 ? "0" : "1px solid var(--line)",
          gap: 24,
        }}>
          <div style={{ flex: 1 }}>
            <div style={{ color: "var(--t0)", fontSize: 12, fontWeight: 700, marginBottom: 4 }}>
              {item.title}
            </div>
            <div style={{ color: "var(--t2)", fontSize: 11, lineHeight: 1.5 }}>
              {item.body}
            </div>
          </div>
          {item.danger
            ? <DangerButton disabled>Coming soon</DangerButton>
            : <GhostButton disabled>Coming soon</GhostButton>
          }
        </div>
      ))}
    </Section>
  );
}

// ─── Inputs + buttons ───────────────────────────────────────────────────────

function TextInput({ value, onChange, placeholder, dirty }: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  dirty?: boolean;
}) {
  const baseBorder = dirty ? "var(--amber)" : "var(--line2)";
  const baseBoxShadow = dirty ? "0 0 0 3px rgba(240, 165, 0, 0.08)" : "none";
  const inputRef = useRef<HTMLInputElement | null>(null);
  // Keep the border in sync if the dirty prop changes while the input
  // is focused (e.g. user types and the form computes dirty=true after
  // the keystroke). onFocus/onBlur set inline styles, so we have to
  // re-apply when dirty toggles.
  useEffect(() => {
    const el = inputRef.current;
    if (!el || document.activeElement === el) return;
    el.style.borderColor = baseBorder;
    el.style.boxShadow = baseBoxShadow;
  }, [baseBorder, baseBoxShadow]);

  return (
    <input
      ref={inputRef}
      type="text"
      value={value}
      placeholder={placeholder}
      onChange={(e) => onChange(e.currentTarget.value)}
      onFocus={(e) => {
        e.currentTarget.style.borderColor = "var(--green)";
        e.currentTarget.style.boxShadow = "0 0 0 3px var(--green-soft)";
      }}
      onBlur={(e) => {
        e.currentTarget.style.borderColor = baseBorder;
        e.currentTarget.style.boxShadow = baseBoxShadow;
      }}
      style={{
        width: "100%",
        maxWidth: 420,
        background: "var(--bg0)",
        border: `1px solid ${baseBorder}`,
        color: "var(--t0)",
        fontFamily: "inherit",
        fontSize: 13,
        padding: "10px 12px",
        borderRadius: 2,
        outline: "none",
        boxShadow: baseBoxShadow,
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
        padding: "9px 18px",
        fontFamily: "inherit",
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: "0.12em",
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
        padding: "9px 18px",
        fontFamily: "inherit",
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        border: `1px solid ${disabled ? "var(--line)" : "var(--line2)"}`,
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

function DangerButton({ children, disabled, onClick }: {
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
        e.currentTarget.style.background = "var(--red)";
        e.currentTarget.style.color = "#1a0000";
      }}
      onMouseLeave={(e) => {
        if (disabled) return;
        e.currentTarget.style.background = "transparent";
        e.currentTarget.style.color = "var(--red)";
      }}
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "9px 18px",
        fontFamily: "inherit",
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        border: `1px solid ${disabled ? "var(--line)" : "var(--red)"}`,
        borderRadius: 2,
        background: "transparent",
        color: disabled ? "var(--t3)" : "var(--red)",
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

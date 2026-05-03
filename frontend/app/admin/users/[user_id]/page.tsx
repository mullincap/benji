"use client";

/**
 * frontend/app/admin/users/[user_id]/page.tsx
 * =============================================
 * User detail. Two-column layout per the mockup:
 *
 *   LEFT  — identity card + meta list + actions stack
 *   RIGHT — tabbed content (Allocations / Capital Events / Sessions / Connections)
 *
 * Modal-driven actions (reset password, lock, revoke sessions) land in
 * commit 5. This commit only renders the read-only views.
 *
 * Data fetching strategy:
 *   - User detail loads on mount (always shown in left column).
 *   - Each tab loads its data on first activation, cached in component
 *     state. Switching tabs after first load doesn't refetch.
 */

import { useEffect, useState, use } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";

import { AdminCard, AdminTable, Avatar, StatusPill, TerminalStatusBar } from "../../_components";
import { type Column } from "../../_components/AdminTable";
import { deriveInitials } from "../../_components/Avatar";
import ResetPasswordModal from "../../_components/ResetPasswordModal";
import {
  type Allocation,
  type CapitalEvent,
  type ConnectionRow,
  type SessionRow,
  type UserDetail,
  adminLockUser,
  adminRevokeSessions,
  adminUnlockUser,
  fetchUserAllocations,
  fetchUserCapitalEvents,
  fetchUserConnections,
  fetchUserDetail,
  fetchUserSessions,
} from "../../_lib/api";
import {
  formatAbsoluteDate,
  formatPct,
  formatRelative,
  formatUsd,
  statusDisplay,
} from "../../_lib/format";

type TabKey = "allocations" | "capital_events" | "sessions" | "connections";

const TABS: Array<{ key: TabKey; label: string }> = [
  { key: "allocations",   label: "Allocations" },
  { key: "capital_events", label: "Capital Events" },
  { key: "sessions",      label: "Sessions" },
  { key: "connections",   label: "Connections" },
];

export default function UserDetailPage({
  params,
}: {
  params: Promise<{ user_id: string }>;
}) {
  // Next.js 16: page params are a Promise. `use()` unwraps in client components.
  const { user_id } = use(params);
  const router = useRouter();

  const [user, setUser] = useState<UserDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  const [activeTab, setActiveTab] = useState<TabKey>("allocations");

  const [resetOpen, setResetOpen] = useState(false);
  const [actionToast, setActionToast] = useState<{ kind: "ok" | "err"; msg: string } | null>(null);
  const [actionInflight, setActionInflight] = useState<string | null>(null);

  // Auto-clear toast after 4s.
  useEffect(() => {
    if (!actionToast) return;
    const id = window.setTimeout(() => setActionToast(null), 4000);
    return () => window.clearTimeout(id);
  }, [actionToast]);

  async function handleRevokeSessions() {
    if (actionInflight || !user) return;
    if (!window.confirm(`Revoke all active sessions for ${user.email}? They will be signed out immediately on every device.`)) return;
    setActionInflight("revoke");
    try {
      const r = await adminRevokeSessions(user.user_id);
      setActionToast({ kind: "ok", msg: `Revoked ${r.sessions_revoked} session${r.sessions_revoked === 1 ? "" : "s"}.` });
      // Refresh detail counts.
      const refreshed = await fetchUserDetail(user.user_id);
      setUser(refreshed);
      // Sessions tab cache is stale — clear so next visit re-fetches.
      setSessions(null);
    } catch (err) {
      setActionToast({ kind: "err", msg: err instanceof Error ? err.message : String(err) });
    } finally {
      setActionInflight(null);
    }
  }

  async function handleLockToggle() {
    if (actionInflight || !user) return;
    const isCurrentlyLocked = user.locked_until != null && new Date(user.locked_until) > new Date();
    const verb = isCurrentlyLocked ? "Unlock" : "Lock";
    if (!window.confirm(`${verb} ${user.email}?${isCurrentlyLocked ? "" : " They will not be able to sign in for 24 hours."}`)) return;
    setActionInflight("lock");
    try {
      if (isCurrentlyLocked) {
        await adminUnlockUser(user.user_id);
        setActionToast({ kind: "ok", msg: "Account unlocked." });
      } else {
        await adminLockUser(user.user_id, 24);
        setActionToast({ kind: "ok", msg: "Account locked for 24 hours." });
      }
      const refreshed = await fetchUserDetail(user.user_id);
      setUser(refreshed);
    } catch (err) {
      setActionToast({ kind: "err", msg: err instanceof Error ? err.message : String(err) });
    } finally {
      setActionInflight(null);
    }
  }

  async function handleResetClosed(generated: boolean) {
    setResetOpen(false);
    if (generated && user) {
      // Refresh after reset to pick up password_is_temporary + revoked
      // session counts. The temp password value itself is shown only
      // inside the modal — never persisted in this component's state.
      try {
        const refreshed = await fetchUserDetail(user.user_id);
        setUser(refreshed);
      } catch {
        // best-effort
      }
      setSessions(null);
    }
  }

  // Per-tab caches. null = not yet loaded; [] = loaded but empty.
  const [allocations, setAllocations] = useState<Allocation[] | null>(null);
  const [capitalEvents, setCapitalEvents] = useState<CapitalEvent[] | null>(null);
  const [sessions, setSessions] = useState<SessionRow[] | null>(null);
  const [connections, setConnections] = useState<ConnectionRow[] | null>(null);
  const [tabError, setTabError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await fetchUserDetail(user_id);
        if (!cancelled) setUser(data);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [user_id]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      // Clear any stale error from a previous tab when entering this
      // effect — but inside the async IIFE so the render-pure rule is
      // satisfied (tabError clear is treated as a side effect, not a
      // render-time mutation).
      setTabError(null);
      try {
        if (activeTab === "allocations" && allocations == null) {
          const r = await fetchUserAllocations(user_id);
          if (!cancelled) setAllocations(r.allocations);
        } else if (activeTab === "capital_events" && capitalEvents == null) {
          const r = await fetchUserCapitalEvents(user_id);
          if (!cancelled) setCapitalEvents(r.events);
        } else if (activeTab === "sessions" && sessions == null) {
          const r = await fetchUserSessions(user_id);
          if (!cancelled) setSessions(r.sessions);
        } else if (activeTab === "connections" && connections == null) {
          const r = await fetchUserConnections(user_id);
          if (!cancelled) setConnections(r.connections);
        }
      } catch (err) {
        if (!cancelled) {
          setTabError(err instanceof Error ? err.message : String(err));
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [activeTab, user_id, allocations, capitalEvents, sessions, connections]);

  if (error) {
    return (
      <div style={{ color: "var(--red)", padding: 24 }}>
        Failed to load user: {error}
      </div>
    );
  }

  if (!user) {
    return (
      <div style={{ color: "var(--t3)", padding: 24, fontSize: 11, letterSpacing: "0.12em", textTransform: "uppercase" }}>
        Loading user…
      </div>
    );
  }

  const fullName = [user.first_name, user.last_name].filter(Boolean).join(" ") || user.email;
  const userIdShort = user.user_id.slice(0, 4) + "…" + user.user_id.slice(-4);

  return (
    <div>
      {/* ─── Page head ────────────────────────────────────────────────────── */}
      <div
        style={{
          display: "flex",
          alignItems: "flex-end",
          justifyContent: "space-between",
          marginBottom: 24,
          paddingBottom: 16,
          borderBottom: "1px solid var(--line)",
        }}
      >
        <div>
          <div style={{ color: "var(--t3)", fontSize: 11, marginBottom: 6 }}>
            <Link href="/admin/users" style={{ color: "var(--t2)", textDecoration: "none" }}>
              Admin
            </Link>{" "}
            /{" "}
            <Link href="/admin/users" style={{ color: "var(--t2)", textDecoration: "none" }}>
              Users
            </Link>{" "}
            / {fullName}
          </div>
          <h1
            style={{
              fontSize: 22,
              fontWeight: 700,
              color: "var(--t0)",
              letterSpacing: "-0.01em",
              margin: 0,
            }}
          >
            {fullName}{" "}
            <span style={{ color: "var(--t3)", fontWeight: 400, marginLeft: 12, fontSize: 14 }}>
              user_id {userIdShort}
            </span>
          </h1>
        </div>
        <div>
          <button
            onClick={() => router.push("/admin/users")}
            style={{
              background: "transparent",
              color: "var(--t0)",
              border: "1px solid var(--line2)",
              borderRadius: 2,
              padding: "6px 10px",
              fontFamily: "inherit",
              fontSize: 10,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              fontWeight: 700,
              cursor: "pointer",
            }}
          >
            ← Back to Users
          </button>
        </div>
      </div>

      <TerminalStatusBar
        left={
          <>
            &gt; user.status ={" "}
            <span style={{ color: "var(--amber)" }}>
              {user.is_active ? "active" : "deactivated"}
            </span>{" "}
            · email_verified ={" "}
            <span style={{ color: "var(--amber)" }}>{String(user.email_verified)}</span>{" "}
            · admin = <span style={{ color: "var(--t3)" }}>{String(user.is_admin)}</span>
            {user.password_is_temporary && (
              <>
                {" "}
                · password ={" "}
                <span style={{ color: "var(--amber)" }}>temporary</span>
              </>
            )}
          </>
        }
        right={
          user.last_login
            ? `last login ${formatRelative(user.last_login)}`
            : "never logged in"
        }
      />

      {actionToast && (
        <div
          role="status"
          aria-live="polite"
          style={{
            marginBottom: 16,
            padding: "10px 14px",
            background: actionToast.kind === "ok" ? "rgba(0, 200, 150, 0.08)" : "rgba(239, 68, 68, 0.06)",
            border: `1px solid ${actionToast.kind === "ok" ? "rgba(0, 200, 150, 0.4)" : "rgba(239, 68, 68, 0.4)"}`,
            borderLeft: `2px solid ${actionToast.kind === "ok" ? "var(--green)" : "var(--red)"}`,
            borderRadius: 2,
            color: actionToast.kind === "ok" ? "var(--green)" : "var(--red)",
            fontSize: 12,
          }}
        >
          {actionToast.msg}
        </div>
      )}

      {/* ─── Two-column layout ────────────────────────────────────────────── */}
      <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: 20, alignItems: "flex-start" }}>
        {/* LEFT: identity card + meta */}
        <AdminCard flush>
          <IdentityBlock user={user} />
          <MetaList user={user} />
          <ActionsList
            user={user}
            inflight={actionInflight}
            onResetPassword={() => setResetOpen(true)}
            onRevokeSessions={handleRevokeSessions}
            onLockToggle={handleLockToggle}
          />
        </AdminCard>

        {/* RIGHT: tabs */}
        <div>
          <div style={{ display: "flex", borderBottom: "1px solid var(--line)", marginBottom: 14 }}>
            {TABS.map((t) => {
              const isActive = activeTab === t.key;
              const count =
                t.key === "allocations" ? user.allocations_total :
                t.key === "sessions" ? user.sessions_active :
                t.key === "connections" ? user.connections_total :
                undefined;
              return (
                <button
                  key={t.key}
                  onClick={() => setActiveTab(t.key)}
                  style={{
                    padding: "10px 16px",
                    fontSize: 10,
                    letterSpacing: "0.16em",
                    textTransform: "uppercase",
                    color: isActive ? "var(--amber)" : "var(--t3)",
                    background: "transparent",
                    border: 0,
                    borderBottom: isActive ? "2px solid var(--amber)" : "2px solid transparent",
                    cursor: "pointer",
                    fontFamily: "inherit",
                    fontWeight: 700,
                    marginBottom: -1,
                  }}
                >
                  {t.label}
                  {count != null && (
                    <span style={{ marginLeft: 6, color: "var(--t3)", fontWeight: 400 }}>{count}</span>
                  )}
                </button>
              );
            })}
          </div>

          {tabError ? (
            <div role="alert" style={{ color: "var(--red)", padding: 12 }}>
              Failed to load {activeTab}: {tabError}
            </div>
          ) : activeTab === "allocations" ? (
            <AllocationsTab rows={allocations} />
          ) : activeTab === "capital_events" ? (
            <CapitalEventsTab rows={capitalEvents} />
          ) : activeTab === "sessions" ? (
            <SessionsTab rows={sessions} />
          ) : (
            <ConnectionsTab rows={connections} />
          )}
        </div>
      </div>

      {resetOpen && user && (
        <ResetPasswordModal
          userId={user.user_id}
          userEmail={user.email}
          userName={[user.first_name, user.last_name].filter(Boolean).join(" ") || user.email}
          onClose={() => {
            void handleResetClosed(true);
          }}
        />
      )}
    </div>
  );
}

// ─── Identity card + meta list (left column) ──────────────────────────────

function IdentityBlock({ user }: { user: UserDetail }) {
  const initials = deriveInitials({
    first_name: user.first_name,
    last_name: user.last_name,
    email: user.email,
  });
  const statusBundle = statusDisplay(
    user.is_admin
      ? "admin"
      : user.locked_until && new Date(user.locked_until) > new Date()
      ? "locked"
      : !user.last_login
      ? "pending"
      : "active",
  );
  const fullName = [user.first_name, user.last_name].filter(Boolean).join(" ") || "—";

  return (
    <div
      style={{
        textAlign: "center",
        padding: "24px 16px 18px",
        borderBottom: "1px solid var(--line)",
      }}
    >
      <div style={{ display: "flex", justifyContent: "center", marginBottom: 14 }}>
        <Avatar size="lg" initials={initials} />
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color: "var(--t0)", marginBottom: 4 }}>
        {fullName}
      </div>
      <div style={{ color: "var(--t2)", fontSize: 12, marginBottom: 12 }}>{user.email}</div>
      <div style={{ display: "flex", gap: 6, justifyContent: "center", flexWrap: "wrap" }}>
        <StatusPill tone={statusBundle.tone}>{statusBundle.label}</StatusPill>
        {user.role && <StatusPill tone="dim" noDot>{user.role}</StatusPill>}
      </div>
    </div>
  );
}

function MetaList({ user }: { user: UserDetail }) {
  const items: Array<{ k: string; v: React.ReactNode }> = [
    { k: "Firm", v: user.firm || "—" },
    { k: "Role", v: user.role || "—" },
    { k: "Joined", v: formatAbsoluteDate(user.created_at) },
    { k: "Last Login", v: formatRelative(user.last_login) },
    { k: "Last IP", v: user.last_ip || "—" },
    { k: "Sessions", v: `${user.sessions_active} active` },
    {
      k: "Failed Logins",
      v: (
        <span style={{ color: user.failed_login_count > 0 ? "var(--amber)" : "var(--green)" }}>
          {user.failed_login_count}
        </span>
      ),
    },
    {
      k: "Email Verified",
      v: (
        <span style={{ color: user.email_verified ? "var(--green)" : "var(--amber)" }}>
          {user.email_verified ? "Yes" : "No"}
        </span>
      ),
    },
    {
      k: "Password",
      v: user.password_is_temporary ? (
        <span style={{ color: "var(--amber)" }}>Temporary</span>
      ) : (
        <span style={{ color: "var(--t2)" }}>
          set {formatRelative(user.password_set_at)}
          {user.password_changed_by_email ? ` by ${user.password_changed_by_email}` : " by self"}
        </span>
      ),
    },
  ];

  return (
    <ul style={{ listStyle: "none", padding: "14px 0", margin: 0 }}>
      {items.map((it) => (
        <li
          key={it.k}
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "7px 16px",
            fontSize: 12,
          }}
        >
          <span style={{ color: "var(--t3)", letterSpacing: "0.04em" }}>{it.k}</span>
          <span style={{ color: "var(--t0)", fontVariantNumeric: "tabular-nums" }}>{it.v}</span>
        </li>
      ))}
    </ul>
  );
}

function ActionsList({
  user,
  inflight,
  onResetPassword,
  onRevokeSessions,
  onLockToggle,
}: {
  user: UserDetail;
  inflight: string | null;
  onResetPassword: () => void;
  onRevokeSessions: () => void;
  onLockToggle: () => void;
}) {
  const isLocked = user.locked_until != null && new Date(user.locked_until) > new Date();
  const baseBtnStyle: React.CSSProperties = {
    width: "100%",
    background: "transparent",
    color: "var(--t0)",
    border: "1px solid var(--line2)",
    borderRadius: 2,
    padding: "10px 12px",
    fontFamily: "inherit",
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: "0.12em",
    textTransform: "uppercase",
    cursor: "pointer",
    textAlign: "left",
  };

  return (
    <div
      style={{
        borderTop: "1px solid var(--line)",
        padding: 14,
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}
    >
      <button
        type="button"
        onClick={onResetPassword}
        disabled={inflight !== null}
        style={{ ...baseBtnStyle, opacity: inflight ? 0.6 : 1 }}
      >
        ⏳ Reset Password
      </button>
      <button
        type="button"
        onClick={onRevokeSessions}
        disabled={inflight !== null || user.sessions_active === 0}
        style={{
          ...baseBtnStyle,
          opacity: inflight === "revoke" ? 0.6 : user.sessions_active === 0 ? 0.4 : 1,
          cursor: user.sessions_active === 0 ? "not-allowed" : "pointer",
        }}
      >
        ↺ Revoke All Sessions
        {user.sessions_active > 0 && (
          <span style={{ color: "var(--t3)", fontWeight: 400, marginLeft: 6, fontSize: 10 }}>
            ({user.sessions_active})
          </span>
        )}
      </button>
      <button
        type="button"
        onClick={onLockToggle}
        disabled={inflight !== null}
        style={{
          ...baseBtnStyle,
          color: isLocked ? "var(--green)" : "var(--red)",
          borderColor: isLocked ? "rgba(0,200,150,0.4)" : "rgba(239,68,68,0.4)",
          opacity: inflight === "lock" ? 0.6 : 1,
        }}
      >
        {isLocked ? "🔓 Unlock Account" : "🔒 Lock Account (24h)"}
      </button>
    </div>
  );
}

// ─── Tab panels ────────────────────────────────────────────────────────────

function AllocationsTab({ rows }: { rows: Allocation[] | null }) {
  const columns: Column<Allocation>[] = [
    {
      header: "Strategy",
      width: "32%",
      render: (a) => (
        <div>
          <div style={{ color: "var(--t0)", fontWeight: 700 }}>
            {a.strategy_name || "(unknown strategy)"}
            {a.version_label && (
              <span style={{ color: "var(--t3)", fontWeight: 400, marginLeft: 6, fontSize: 10 }}>
                {a.version_label}
              </span>
            )}
          </div>
          <div style={{ color: "var(--t3)", fontSize: 10 }}>
            alloc {a.allocation_id.slice(0, 4)}…{a.allocation_id.slice(-4)} · {a.exchange}
            {a.connection_label ? ` · ${a.connection_label}` : ""}
          </div>
        </div>
      ),
    },
    {
      header: "Status",
      render: (a) => {
        if (a.status === "active") return <StatusPill tone="green">Active</StatusPill>;
        if (a.status === "closed") return <StatusPill tone="dim">Closed</StatusPill>;
        return <StatusPill tone="amber">{a.status}</StatusPill>;
      },
    },
    {
      header: "Started",
      render: (a) => (
        <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
          {formatAbsoluteDate(a.created_at)}
        </span>
      ),
    },
    {
      header: "Capital",
      alignRight: true,
      render: (a) => <span style={{ fontVariantNumeric: "tabular-nums" }}>{formatUsd(a.capital_usd)}</span>,
    },
    {
      header: "Equity",
      alignRight: true,
      render: (a) => {
        if (a.current_equity_usd == null) return <span style={{ color: "var(--t3)" }}>—</span>;
        const positive = a.return_pct != null && a.return_pct >= 0;
        return (
          <span
            style={{
              fontVariantNumeric: "tabular-nums",
              color: positive ? "var(--green)" : "var(--red)",
            }}
          >
            {formatUsd(a.current_equity_usd)}
          </span>
        );
      },
    },
    {
      header: "Return",
      alignRight: true,
      render: (a) => {
        if (a.return_pct == null) return <span style={{ color: "var(--t3)" }}>—</span>;
        return (
          <span
            style={{
              fontVariantNumeric: "tabular-nums",
              color: a.return_pct >= 0 ? "var(--green)" : "var(--red)",
            }}
          >
            {formatPct(a.return_pct)}
          </span>
        );
      },
    },
    {
      header: "Last Bar",
      alignRight: true,
      render: (a) => (
        <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
          {formatRelative(a.equity_at)}
        </span>
      ),
    },
  ];

  return (
    <AdminTable<Allocation>
      columns={columns}
      rows={rows || []}
      loading={rows == null}
      rowKey={(a) => a.allocation_id}
      emptyMessage="No allocations for this user."
    />
  );
}

function CapitalEventsTab({ rows }: { rows: CapitalEvent[] | null }) {
  const columns: Column<CapitalEvent>[] = [
    {
      header: "When",
      render: (e) => (
        <span style={{ color: "var(--t2)", fontVariantNumeric: "tabular-nums" }}>
          {formatRelative(e.event_at)}
        </span>
      ),
    },
    {
      header: "Kind",
      render: (e) => (
        <StatusPill tone={e.kind === "deposit" ? "green" : "amber"} noDot>
          {e.kind}
        </StatusPill>
      ),
    },
    {
      header: "Amount",
      alignRight: true,
      render: (e) => (
        <span
          style={{
            fontVariantNumeric: "tabular-nums",
            color: e.kind === "deposit" ? "var(--green)" : "var(--amber)",
          }}
        >
          {e.kind === "withdrawal" ? "-" : "+"}
          {formatUsd(e.amount_usd, true)}
        </span>
      ),
    },
    {
      header: "Source",
      render: (e) => <span style={{ color: "var(--t3)" }}>{e.source}</span>,
    },
    {
      header: "Connection",
      render: (e) =>
        e.exchange ? (
          <span style={{ color: "var(--t2)" }}>
            {e.exchange}
            {e.connection_label ? ` · ${e.connection_label}` : ""}
          </span>
        ) : (
          <span style={{ color: "var(--t3)" }}>—</span>
        ),
    },
  ];

  return (
    <AdminTable<CapitalEvent>
      columns={columns}
      rows={rows || []}
      loading={rows == null}
      rowKey={(e) => e.event_id}
      emptyMessage="No capital events recorded."
    />
  );
}

function SessionsTab({ rows }: { rows: SessionRow[] | null }) {
  const columns: Column<SessionRow>[] = [
    {
      header: "Started",
      render: (s) => (
        <span style={{ color: "var(--t2)", fontVariantNumeric: "tabular-nums" }}>
          {formatRelative(s.created_at)}
        </span>
      ),
    },
    {
      header: "Expires",
      render: (s) => (
        <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
          {s.expires_at ? formatAbsoluteDate(s.expires_at) : "—"}
        </span>
      ),
    },
    {
      header: "Status",
      render: (s) => (
        <StatusPill tone={s.is_active ? "green" : "dim"}>
          {s.is_active ? "Active" : "Expired"}
        </StatusPill>
      ),
    },
  ];

  return (
    <AdminTable<SessionRow>
      columns={columns}
      rows={rows || []}
      loading={rows == null}
      rowKey={(s, i) => `${s.created_at || ""}_${i}`}
      emptyMessage="No active sessions."
    />
  );
}

function ConnectionsTab({ rows }: { rows: ConnectionRow[] | null }) {
  const columns: Column<ConnectionRow>[] = [
    {
      header: "Exchange",
      render: (c) => (
        <span style={{ color: "var(--t0)", fontWeight: 700 }}>
          {c.exchange}
          {c.testnet && (
            <span style={{ color: "var(--amber)", fontWeight: 400, marginLeft: 6, fontSize: 10 }}>
              [testnet]
            </span>
          )}
        </span>
      ),
    },
    { header: "Label", render: (c) => c.label || "—" },
    {
      header: "Status",
      render: (c) => {
        const tone = c.status === "active" ? "green" : c.status === "errored" || c.status === "invalid" ? "red" : c.status === "revoked" ? "dim" : "amber";
        return <StatusPill tone={tone}>{c.status}</StatusPill>;
      },
    },
    {
      header: "Validated",
      render: (c) => (
        <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
          {formatRelative(c.last_validated_at)}
        </span>
      ),
    },
    {
      header: "Principal",
      alignRight: true,
      render: (c) => (
        <span style={{ fontVariantNumeric: "tabular-nums", color: "var(--t2)" }}>
          {c.principal_baseline_usd != null ? formatUsd(c.principal_baseline_usd) : "—"}
        </span>
      ),
    },
  ];

  return (
    <AdminTable<ConnectionRow>
      columns={columns}
      rows={rows || []}
      loading={rows == null}
      rowKey={(c) => c.connection_id}
      emptyMessage="No exchange connections."
    />
  );
}

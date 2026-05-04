"use client";

/**
 * frontend/app/admin/invitations/page.tsx
 * =========================================
 * /admin/invitations — outstanding + accepted + expired invitations.
 *
 * Stats strip: pending / accepted / expired / acceptance rate. Filter
 * pills mirror the mockup: All / Pending / Accepted / Expired.
 *
 * "Copy Link" on existing rows is intentionally NOT a copyable URL —
 * tokens are SHA-256 hashed at storage time, so the plaintext URL
 * cannot be reconstructed. Clicking opens an explainer modal pointing
 * the admin at "Generate New" instead.
 */

import { useEffect, useState } from "react";

import { useConfirm } from "../../components/ConfirmDialog";
import {
  AdminTable,
  Avatar,
  FilterPill,
  StatusPill,
} from "../_components";
import { type Column } from "../_components/AdminTable";
import { deriveInitials } from "../_components/Avatar";
import IssueInviteModal from "../_components/IssueInviteModal";
import {
  type Invitation,
  type InvitationStatus,
  fetchInvitations,
  revokeInvitation,
} from "../_lib/api";
import { formatAbsoluteDate, formatRelative } from "../_lib/format";

const STATUS_FILTERS: Array<{ key: InvitationStatus | "all"; label: string }> = [
  { key: "all", label: "All" },
  { key: "pending", label: "Pending" },
  { key: "accepted", label: "Accepted" },
  { key: "expired", label: "Expired" },
];

export default function InvitationsPage() {
  const confirm = useConfirm();
  const [statusFilter, setStatusFilter] = useState<InvitationStatus | "all">("all");
  const [invitations, setInvitations] = useState<Invitation[] | null>(null);
  const [stats, setStats] = useState<{
    pending: number;
    accepted: number;
    expired: number;
    acceptance_rate: number | null;
  } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [issueOpen, setIssueOpen] = useState(false);
  const [actionInflight, setActionInflight] = useState<string | null>(null);
  const [actionToast, setActionToast] = useState<{ kind: "ok" | "err"; msg: string } | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    if (!actionToast) return;
    const id = window.setTimeout(() => setActionToast(null), 4000);
    return () => window.clearTimeout(id);
  }, [actionToast]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        const data = await fetchInvitations({
          status: statusFilter === "all" ? undefined : statusFilter,
        });
        if (cancelled) return;
        setInvitations(data.invitations);
        setStats({
          pending: data.stats.pending,
          accepted: data.stats.accepted,
          expired: data.stats.expired,
          acceptance_rate: data.stats.acceptance_rate,
        });
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [statusFilter, reloadKey]);

  async function handleRevoke(inv: Invitation) {
    if (actionInflight) return;
    const ok = await confirm({
      eyebrow: "Admin · Confirm",
      title: "Revoke invitation?",
      description: `${inv.invited_email} will not be able to use this invitation link. You can issue a new one anytime.`,
      confirmLabel: "Revoke invitation",
      destructive: true,
    });
    if (!ok) return;
    setActionInflight(inv.invitation_id);
    try {
      await revokeInvitation(inv.invitation_id);
      setActionToast({ kind: "ok", msg: `Revoked invite for ${inv.invited_email}.` });
      setReloadKey((k) => k + 1);
    } catch (err) {
      setActionToast({
        kind: "err",
        msg: err instanceof Error ? err.message : String(err),
      });
    } finally {
      setActionInflight(null);
    }
  }

  function handleCopyExpiredHint(inv: Invitation) {
    setActionToast({
      kind: "err",
      msg: `Tokens are stored hashed — the original URL for ${inv.invited_email} cannot be recovered. Use "New Invitation" to issue a fresh one.`,
    });
  }

  const columns: Column<Invitation>[] = [
    {
      header: "Invited Email",
      width: "26%",
      render: (inv) => (
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <Avatar size="sm" initials={deriveInitials({ first_name: null, last_name: null, email: inv.invited_email })} />
          <div>
            <div style={{ color: "var(--t0)", fontWeight: 700 }}>{inv.invited_email}</div>
            <div style={{ color: "var(--t2)", fontSize: 11 }}>{inv.inviter_firm}</div>
          </div>
        </div>
      ),
    },
    {
      header: "Inviter",
      render: (inv) => (
        <span style={{ color: "var(--t2)" }}>
          {inv.inviter_name}
          {inv.inviter_email && (
            <span style={{ color: "var(--t3)", marginLeft: 6, fontSize: 10 }}>
              ({inv.inviter_email})
            </span>
          )}
        </span>
      ),
    },
    {
      header: "Sent",
      render: (inv) => (
        <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
          {formatAbsoluteDate(inv.created_at)}
        </span>
      ),
    },
    {
      header: "Expires / Resolved",
      render: (inv) => {
        if (inv.status === "accepted") {
          return (
            <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
              accepted {formatRelative(inv.accepted_at)}
            </span>
          );
        }
        if (inv.status === "expired") {
          return (
            <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
              expired {formatAbsoluteDate(inv.expires_at)}
            </span>
          );
        }
        // pending or expiring
        return (
          <span
            style={{
              color: inv.status === "expiring" ? "var(--red)" : "var(--t3)",
              fontVariantNumeric: "tabular-nums",
            }}
          >
            {formatRelative(inv.expires_at)}
          </span>
        );
      },
    },
    {
      header: "Status",
      render: (inv) => {
        if (inv.status === "accepted") return <StatusPill tone="green">Accepted</StatusPill>;
        if (inv.status === "expired") return <StatusPill tone="dim">Expired</StatusPill>;
        if (inv.status === "expiring") return <StatusPill tone="red">Expiring</StatusPill>;
        return <StatusPill tone="amber">Pending</StatusPill>;
      },
    },
    {
      header: "Actions",
      alignRight: true,
      render: (inv) => {
        if (inv.status === "accepted" || inv.status === "expired") {
          return (
            <button
              type="button"
              onClick={() => handleCopyExpiredHint(inv)}
              style={smallBtn}
              title="Token cannot be recovered — generate a new invite"
            >
              Why no link?
            </button>
          );
        }
        // pending or expiring
        return (
          <button
            type="button"
            onClick={() => handleRevoke(inv)}
            disabled={actionInflight === inv.invitation_id}
            style={{ ...smallBtn, color: "var(--red)", borderColor: "rgba(239,68,68,0.4)" }}
          >
            {actionInflight === inv.invitation_id ? "…" : "Revoke"}
          </button>
        );
      },
    },
  ];

  return (
    <div>
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
            Admin / Invitations
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
            Invitations
            {stats && (
              <span style={{ color: "var(--t3)", fontWeight: 400, marginLeft: 12, fontSize: 14 }}>
                {stats.pending} pending · {stats.accepted} accepted · {stats.expired} expired
              </span>
            )}
          </h1>
        </div>
        <button
          type="button"
          onClick={() => setIssueOpen(true)}
          style={{
            background: "var(--amber)",
            color: "#1a1100",
            border: "1px solid var(--amber)",
            borderRadius: 2,
            padding: "9px 14px",
            fontFamily: "inherit",
            fontSize: 11,
            fontWeight: 700,
            letterSpacing: "0.12em",
            textTransform: "uppercase",
            cursor: "pointer",
          }}
        >
          + New Invitation
        </button>
      </div>

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

      {/* Stats strip — 4 tiles per the mockup */}
      {stats && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(4, 1fr)",
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 2,
            marginBottom: 16,
          }}
        >
          <Tile label="Pending"   value={stats.pending}  tone="amber" />
          <Tile label="Accepted"  value={stats.accepted} tone="green" />
          <Tile label="Expired"   value={stats.expired}  tone="dim" />
          <Tile
            label="Acceptance Rate"
            value={stats.acceptance_rate != null ? `${(stats.acceptance_rate * 100).toFixed(0)}%` : "—"}
            tone="default"
            last
          />
        </div>
      )}

      <div
        style={{
          display: "flex",
          gap: 8,
          alignItems: "center",
          padding: "10px 14px",
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderBottom: 0,
          borderRadius: "2px 2px 0 0",
        }}
      >
        {STATUS_FILTERS.map((f) => (
          <FilterPill
            key={f.key}
            active={statusFilter === f.key}
            onClick={() => setStatusFilter(f.key)}
          >
            {f.label}
          </FilterPill>
        ))}
      </div>

      {error ? (
        <div
          role="alert"
          style={{
            padding: 16,
            background: "var(--bg2)",
            border: "1px solid var(--red)",
            borderTop: 0,
            color: "var(--red)",
            fontSize: 12,
          }}
        >
          Failed to load invitations: {error}
        </div>
      ) : (
        <AdminTable<Invitation>
          columns={columns}
          rows={invitations || []}
          loading={loading && invitations == null}
          rowKey={(inv) => inv.invitation_id}
          emptyMessage="No invitations match your filters."
        />
      )}

      {issueOpen && (
        <IssueInviteModal
          onClose={() => setIssueOpen(false)}
          onIssued={() => setReloadKey((k) => k + 1)}
        />
      )}
    </div>
  );
}

// ─── Stats tile (mockup `.invite-stat`) ────────────────────────────────────

function Tile({
  label,
  value,
  tone,
  last,
}: {
  label: string;
  value: React.ReactNode;
  tone: "amber" | "green" | "dim" | "default";
  last?: boolean;
}) {
  const valueColor =
    tone === "amber" ? "var(--amber)" :
    tone === "green" ? "var(--green)" :
    tone === "dim"   ? "var(--t3)" :
    "var(--t0)";
  return (
    <div
      style={{
        padding: "14px 18px",
        borderRight: last ? "0" : "1px solid var(--line)",
      }}
    >
      <div
        style={{
          color: "var(--t3)",
          fontSize: 10,
          letterSpacing: "0.14em",
          textTransform: "uppercase",
          marginBottom: 4,
        }}
      >
        {label}
      </div>
      <div style={{ color: valueColor, fontSize: 18, fontWeight: 700 }}>{value}</div>
    </div>
  );
}

const smallBtn: React.CSSProperties = {
  background: "transparent",
  color: "var(--t0)",
  border: "1px solid var(--line2)",
  borderRadius: 2,
  padding: "5px 9px",
  fontFamily: "inherit",
  fontSize: 10,
  fontWeight: 700,
  letterSpacing: "0.1em",
  textTransform: "uppercase",
  cursor: "pointer",
};

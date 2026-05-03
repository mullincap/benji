"use client";

/**
 * frontend/app/admin/users/page.tsx
 * ==================================
 * /admin/users — main admin entry point.
 *
 * KPI strip on top, filter pills + search row, then the user table.
 * Click a row to navigate to /admin/users/{user_id}.
 *
 * Auth-bootstrap race defense: the parent admin layout already gates
 * on `loading === false && user.is_admin === true` before rendering
 * children, so by the time this page mounts the API call below is safe
 * to fire.
 */

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

import {
  AdminTable,
  Avatar,
  FilterPill,
  KpiRow,
  StatusPill,
  TerminalStatusBar,
} from "../_components";
import { type Column } from "../_components/AdminTable";
import { deriveInitials } from "../_components/Avatar";
import IssueInviteModal from "../_components/IssueInviteModal";
import {
  fetchUsers,
  type UserStatus,
  type UserSummary,
} from "../_lib/api";
import {
  formatAbsoluteDate,
  formatRelative,
  formatUsd,
  statusDisplay,
} from "../_lib/format";

const STATUS_FILTERS: Array<{ key: UserStatus | "all"; label: string }> = [
  { key: "all", label: "All" },
  { key: "active", label: "Active" },
  { key: "locked", label: "Locked" },
  { key: "admin", label: "Admin" },
  { key: "no_activity", label: "No Allocations" },
];

export default function UsersPage() {
  const router = useRouter();

  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<UserStatus | "all">("all");
  const [issueOpen, setIssueOpen] = useState(false);
  const [users, setUsers] = useState<UserSummary[] | null>(null);
  const [stats, setStats] = useState<{
    total: number;
    active_30d: number;
    pending: number;
    locked: number;
    allocations_total: number;
    capital_deployed_total_usd: number;
  } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshedAt, setRefreshedAt] = useState<Date | null>(null);

  // Debounced search — keeps typing responsive but throttles API hits.
  const debouncedSearch = useDebouncedValue(search, 250);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        const data = await fetchUsers({
          search: debouncedSearch || undefined,
          status: statusFilter === "all" ? undefined : statusFilter,
        });
        if (cancelled) return;
        setUsers(data.users);
        setStats({
          total: data.total,
          active_30d: data.stats.active_30d,
          pending: data.stats.pending,
          locked: data.stats.locked,
          allocations_total: data.stats.allocations_total,
          capital_deployed_total_usd: data.stats.capital_deployed_total_usd,
        });
        setRefreshedAt(new Date());
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [debouncedSearch, statusFilter]);

  const columns: Column<UserSummary>[] = useMemo(
    () => [
      {
        header: "User",
        width: "28%",
        render: (u) => (
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <Avatar
              size="sm"
              initials={deriveInitials({
                first_name: u.first_name,
                last_name: u.last_name,
                email: u.email,
              })}
            />
            <div>
              <div style={{ color: "var(--t0)", fontWeight: 700 }}>
                {[u.first_name, u.last_name].filter(Boolean).join(" ") || "—"}
              </div>
              <div style={{ color: "var(--t2)", fontSize: 11 }}>{u.email}</div>
            </div>
          </div>
        ),
      },
      {
        header: "Firm",
        render: (u) => u.firm || "—",
      },
      {
        header: "Role",
        render: (u) => (
          <span
            style={{
              color: "var(--t2)",
              fontSize: 11,
              padding: "2px 7px",
              border: "1px solid var(--line)",
              borderRadius: 2,
              background: "var(--bg3)",
              letterSpacing: "0.04em",
            }}
          >
            {u.role || "—"}
          </span>
        ),
      },
      {
        header: "Joined",
        render: (u) => (
          <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
            {formatAbsoluteDate(u.created_at)}
          </span>
        ),
      },
      {
        header: "Last Login",
        render: (u) => (
          <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
            {formatRelative(u.last_login)}
          </span>
        ),
      },
      {
        header: "Allocations",
        alignRight: true,
        render: (u) => (
          <span style={{ fontVariantNumeric: "tabular-nums" }}>{u.allocations_count}</span>
        ),
      },
      {
        header: "Capital",
        alignRight: true,
        render: (u) => (
          <span
            style={{
              fontVariantNumeric: "tabular-nums",
              color: u.capital_deployed_usd > 0 ? "var(--t0)" : "var(--t3)",
            }}
          >
            {u.capital_deployed_usd > 0 ? formatUsd(u.capital_deployed_usd) : "—"}
          </span>
        ),
      },
      {
        header: "Status",
        render: (u) => {
          const s = statusDisplay(u.status);
          return <StatusPill tone={s.tone}>{s.label}</StatusPill>;
        },
      },
    ],
    [],
  );

  const refreshNote = refreshedAt
    ? `last refresh ${secondsSince(refreshedAt)}s ago`
    : "loading…";

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
            Admin / Users
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
            Users{" "}
            {stats && (
              <span style={{ color: "var(--t3)", fontWeight: 400, marginLeft: 12, fontSize: 14 }}>
                {stats.total} total · {stats.active_30d} active · {stats.locked} locked
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

      <TerminalStatusBar
        left={
          <>
            &gt; admin.session ={" "}
            <span style={{ color: "var(--amber)" }}>authenticated</span> · scope ={" "}
            <span style={{ color: "var(--amber)" }}>full</span>
          </>
        }
        right={refreshNote}
      />

      {stats && (
        <KpiRow
          kpis={[
            { label: "Total Users", value: stats.total },
            { label: "Active 30d", value: stats.active_30d, tone: "green" },
            { label: "Allocations", value: stats.allocations_total },
            {
              label: "Capital Deployed",
              value: formatUsd(stats.capital_deployed_total_usd),
              tone: "amber",
            },
            { label: "Pending", value: stats.pending, tone: "amber" },
          ]}
        />
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
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search by email, name, or firm…"
          style={{
            flex: 1,
            background: "var(--bg3)",
            border: "1px solid var(--line)",
            color: "var(--t0)",
            padding: "7px 10px",
            fontFamily: "inherit",
            fontSize: 12,
            borderRadius: 2,
            outline: "none",
          }}
        />
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
          Failed to load users: {error}
        </div>
      ) : (
        <AdminTable<UserSummary>
          columns={columns}
          rows={users || []}
          loading={loading && users == null}
          rowKey={(u) => u.user_id}
          onRowClick={(u) => router.push(`/admin/users/${u.user_id}`)}
          emptyMessage="No users match your filters."
        />
      )}

      {/* New-invitation modal — same component the /admin/invitations
          page uses. We don't refresh the users list here on success
          because issuing an invitation doesn't create a user (the new
          user only appears once they accept). */}
      {issueOpen && <IssueInviteModal onClose={() => setIssueOpen(false)} />}
    </div>
  );
}

// ─── Helpers ──────────────────────────────────────────────────────────────

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(id);
  }, [value, delayMs]);
  return debounced;
}

function secondsSince(date: Date): number {
  return Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));
}

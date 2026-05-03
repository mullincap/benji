"use client";

/**
 * frontend/app/admin/audit/page.tsx
 * ===================================
 * /admin/audit — append-only log of every admin action.
 *
 * Filters: action type (pills), search (substring on actor or subject
 * email), last 24h shortcut. Default sort is most-recent-first.
 *
 * No mutating actions here — this page is read-only.
 */

import { useEffect, useMemo, useState } from "react";

import {
  ActionTag,
  AdminTable,
  FilterPill,
  TerminalStatusBar,
} from "../_components";
import { type Column } from "../_components/AdminTable";
import { toneForAction } from "../_components/ActionTag";
import { type AuditEvent, fetchAuditEvents } from "../_lib/api";
import { formatRelative } from "../_lib/format";

// Filter pills — server accepts the underlying action_type string.
const ACTION_FILTERS: Array<{ key: string | "all"; label: string }> = [
  { key: "all", label: "All Actions" },
  { key: "password_reset_admin", label: "Resets" },
  { key: "invitation_issued", label: "Invites" },
  { key: "invitation_revoked", label: "Revoked Invites" },
  { key: "user_locked", label: "Locks" },
  { key: "sessions_revoked", label: "Sessions Revoked" },
  { key: "admin_login_attempt_denied", label: "Denied" },
];

export default function AuditPage() {
  const [actionFilter, setActionFilter] = useState<string | "all">("all");
  const [last24h, setLast24h] = useState(false);
  const [search, setSearch] = useState("");
  const [events, setEvents] = useState<AuditEvent[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const debouncedSearch = useDebouncedValue(search, 250);
  const sinceParam = useMemo(
    () => (last24h ? new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString() : undefined),
    [last24h],
  );

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        const data = await fetchAuditEvents({
          action_type: actionFilter === "all" ? undefined : actionFilter,
          since: sinceParam,
          // Search hits both actor and subject — passing the same term
          // to both lets the server OR them.
          actor_email: debouncedSearch || undefined,
          subject_email: debouncedSearch || undefined,
          limit: 200,
        });
        if (cancelled) return;
        setEvents(data.events);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [actionFilter, sinceParam, debouncedSearch]);

  const columns: Column<AuditEvent>[] = [
    {
      header: "Action",
      width: "22%",
      render: (e) => (
        <ActionTag tone={toneForAction(e.action_type)}>{prettyAction(e.action_type)}</ActionTag>
      ),
    },
    {
      header: "Subject",
      render: (e) => <span style={{ color: "var(--t2)" }}>{e.subject_email || "—"}</span>,
    },
    {
      header: "Actor",
      render: (e) => (
        <span style={{ color: e.actor_email ? "var(--t0)" : "var(--t3)" }}>
          {e.actor_email || "system"}
        </span>
      ),
    },
    {
      header: "Details",
      render: (e) => (
        <span style={{ color: "var(--t3)", fontSize: 11 }}>{summaryFor(e)}</span>
      ),
    },
    {
      header: "IP",
      render: (e) => (
        <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
          {e.ip_address || "—"}
        </span>
      ),
    },
    {
      header: "When",
      alignRight: true,
      render: (e) => (
        <span style={{ color: "var(--t3)", fontVariantNumeric: "tabular-nums" }}>
          {formatRelative(e.created_at)}
        </span>
      ),
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
            Admin / Audit Log
          </div>
          <h1 style={{ fontSize: 22, fontWeight: 700, color: "var(--t0)", letterSpacing: "-0.01em", margin: 0 }}>
            Audit Log
            {events && (
              <span style={{ color: "var(--t3)", fontWeight: 400, marginLeft: 12, fontSize: 14 }}>
                {events.length} event{events.length === 1 ? "" : "s"}
                {last24h ? " · last 24h" : ""}
              </span>
            )}
          </h1>
        </div>
      </div>

      <TerminalStatusBar
        left={
          <>
            &gt; audit.scope ={" "}
            <span style={{ color: "var(--amber)" }}>all admin actions</span>{" "}
            · retention = <span style={{ color: "var(--amber)" }}>indefinite</span>
          </>
        }
        right="live · auto-append"
      />

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
          flexWrap: "wrap",
        }}
      >
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search by user, action, or IP…"
          style={{
            flex: 1,
            minWidth: 200,
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
        {ACTION_FILTERS.map((f) => (
          <FilterPill
            key={f.key}
            active={actionFilter === f.key}
            onClick={() => setActionFilter(f.key)}
          >
            {f.label}
          </FilterPill>
        ))}
        <FilterPill active={last24h} onClick={() => setLast24h((v) => !v)}>
          Last 24h
        </FilterPill>
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
          Failed to load audit log: {error}
        </div>
      ) : (
        <AdminTable<AuditEvent>
          columns={columns}
          rows={events || []}
          loading={loading && events == null}
          rowKey={(e) => e.action_id}
          emptyMessage="No audit events match your filters."
        />
      )}
    </div>
  );
}

// ─── Display helpers ──────────────────────────────────────────────────────

function prettyAction(actionType: string): string {
  // Map server enum values to display labels. Keep the canonical
  // backend strings as the source of truth; this is just rendering.
  const map: Record<string, string> = {
    password_reset_admin: "Password Reset",
    password_changed_self: "Password Changed",
    invitation_issued: "Invite Sent",
    invitation_revoked: "Invite Revoked",
    sessions_revoked: "Sessions Revoked",
    admin_login_attempt_denied: "Admin Denied",
    user_locked: "Account Locked",
    user_unlocked: "Account Unlocked",
    admin_granted: "Admin Granted",
    admin_revoked: "Admin Revoked",
  };
  return map[actionType] || actionType;
}

function summaryFor(e: AuditEvent): string {
  const m = e.metadata || {};
  switch (e.action_type) {
    case "password_reset_admin": {
      const n = Number(m["sessions_revoked"] ?? 0);
      return `Temp password issued${n > 0 ? `; ${n} session${n === 1 ? "" : "s"} revoked` : ""}`;
    }
    case "invitation_issued":
      return `Role: ${m["role"] || "—"} · ${m["expires_in_days"] || "?"}d expiry`;
    case "invitation_revoked":
      return "Marked expired";
    case "sessions_revoked":
      return `${m["sessions_revoked"] ?? "?"} session${(m["sessions_revoked"] as number) === 1 ? "" : "s"} revoked`;
    case "user_locked":
      return `${m["duration_hours"] ?? "?"}h lock`;
    case "user_unlocked":
      return "Counter reset";
    case "admin_login_attempt_denied":
      return `Path: ${m["path"] || "—"}`;
    case "password_changed_self":
      return "Self-initiated";
    default:
      return "—";
  }
}

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(id);
  }, [value, delayMs]);
  return debounced;
}

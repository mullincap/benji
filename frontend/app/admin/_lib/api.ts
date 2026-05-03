/**
 * frontend/app/admin/_lib/api.ts
 * ===============================
 * Admin Console API client. Thin typed wrappers around fetch + the
 * apiFetch helper from app/lib (which adds 401-redirect behavior).
 *
 * Types here mirror the Pydantic-free dict shapes returned by
 * backend/app/api/routes/admin_console.py — keep in sync. If the
 * server adds a field, surface it here so the typecheck catches drift.
 */

import { apiFetch } from "../../lib/api-fetch";

// ─── Types ────────────────────────────────────────────────────────────────

export type UserStatus =
  | "active"
  | "locked"
  | "pending"
  | "idle"
  | "no_activity"
  | "admin";

export type UserSummary = {
  user_id: string;
  email: string;
  first_name: string | null;
  last_name: string | null;
  firm: string | null;
  role: string | null;
  is_admin: boolean;
  is_active: boolean;
  created_at: string | null;
  last_login: string | null;
  locked_until: string | null;
  allocations_count: number;
  capital_deployed_usd: number;
  status: UserStatus;
};

export type UsersListResponse = {
  users: UserSummary[];
  total: number;
  stats: {
    active_30d: number;
    pending: number;
    locked: number;
    allocations_total: number;
    capital_deployed_total_usd: number;
  };
};

export type UserDetail = {
  user_id: string;
  email: string;
  first_name: string | null;
  last_name: string | null;
  firm: string | null;
  role: string | null;
  is_admin: boolean;
  is_active: boolean;
  email_verified: boolean;
  password_is_temporary: boolean;
  password_set_at: string | null;
  password_changed_by_email: string | null;
  created_at: string | null;
  updated_at: string | null;
  last_login: string | null;
  last_ip: string | null;
  locked_until: string | null;
  failed_login_count: number;
  sessions_active: number;
  last_session_at: string | null;
  allocations_active: number;
  allocations_total: number;
  capital_active_usd: number;
  connections_total: number;
};

export type Allocation = {
  allocation_id: string;
  status: string;
  strategy_name: string | null;
  version_label: string | null;
  exchange: string;
  connection_label: string | null;
  capital_usd: number;
  current_equity_usd: number | null;
  return_pct: number | null;
  equity_at: string | null;
  created_at: string | null;
  closed_at: string | null;
  compounding_mode: string;
  runtime_phase: string | null;
};

export type CapitalEvent = {
  event_id: string;
  event_at: string | null;
  kind: string;
  amount_usd: number;
  source: string;
  notes: string | null;
  exchange_event_id: string | null;
  allocation_id: string | null;
  exchange: string | null;
  connection_label: string | null;
};

export type SessionRow = {
  created_at: string | null;
  expires_at: string | null;
  is_active: boolean;
};

export type ConnectionRow = {
  connection_id: string;
  exchange: string;
  label: string | null;
  testnet: boolean;
  status: string;
  last_validated_at: string | null;
  last_error_at: string | null;
  last_error_msg: string | null;
  principal_baseline_usd: number | null;
  principal_anchor_at: string | null;
  created_at: string | null;
  updated_at: string | null;
};

// ─── Endpoints ────────────────────────────────────────────────────────────

async function jsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status}${body ? `: ${body}` : ""}`);
  }
  return (await res.json()) as T;
}

export async function fetchUsers(params: {
  search?: string;
  status?: UserStatus | "all";
  sort?: "last_login" | "joined" | "email";
} = {}): Promise<UsersListResponse> {
  const q = new URLSearchParams();
  if (params.search) q.set("search", params.search);
  if (params.status && params.status !== "all") q.set("status", params.status);
  if (params.sort) q.set("sort", params.sort);
  const path = `/api/admin/users${q.toString() ? `?${q}` : ""}`;
  return jsonOrThrow<UsersListResponse>(await apiFetch(path));
}

export async function fetchUserDetail(userId: string): Promise<UserDetail> {
  return jsonOrThrow<UserDetail>(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}`),
  );
}

export async function fetchUserAllocations(userId: string): Promise<{ allocations: Allocation[] }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/allocations`),
  );
}

export async function fetchUserCapitalEvents(userId: string): Promise<{ events: CapitalEvent[] }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/capital-events`),
  );
}

export async function fetchUserSessions(userId: string): Promise<{ sessions: SessionRow[] }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/sessions`),
  );
}

export async function fetchUserConnections(userId: string): Promise<{ connections: ConnectionRow[] }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/connections`),
  );
}

// Mutating actions land in commit 5 — kept declared here for type-import
// convenience by the modal components.

export async function adminResetPassword(
  userId: string,
): Promise<{ ok: boolean; temp_password: string; sessions_revoked: number }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/reset-password`, {
      method: "POST",
    }),
  );
}

export async function adminRevokeSessions(
  userId: string,
): Promise<{ ok: boolean; sessions_revoked: number }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/revoke-sessions`, {
      method: "POST",
    }),
  );
}

export async function adminLockUser(
  userId: string,
  durationHours = 24,
): Promise<{ ok: boolean; locked_until: string | null }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/lock`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ duration_hours: durationHours }),
    }),
  );
}

export async function adminUnlockUser(userId: string): Promise<{ ok: boolean }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/users/${encodeURIComponent(userId)}/unlock`, {
      method: "POST",
    }),
  );
}

// ─── Invitations ──────────────────────────────────────────────────────────

export type InvitationStatus = "pending" | "expiring" | "accepted" | "expired";

export type Invitation = {
  invitation_id: string;
  invited_email: string;
  inviter_name: string;
  inviter_firm: string;
  inviter_email: string | null;
  expires_at: string | null;
  accepted_at: string | null;
  created_at: string | null;
  status: InvitationStatus;
};

export type InvitationsListResponse = {
  invitations: Invitation[];
  total: number;
  stats: {
    pending: number;
    expiring: number;
    accepted: number;
    expired: number;
    acceptance_rate: number | null;
  };
};

export type IssueInviteBody = {
  email: string;
  firm: string | null;
  role: string;
  expires_in_days: number;
};

export type IssueInviteResponse = {
  ok: boolean;
  invitation_id: string;
  invite_url: string;
  expires_at: string;
};

export async function fetchInvitations(params: {
  status?: InvitationStatus | "all";
} = {}): Promise<InvitationsListResponse> {
  const q = new URLSearchParams();
  if (params.status && params.status !== "all") q.set("status", params.status);
  const path = `/api/admin/invitations${q.toString() ? `?${q}` : ""}`;
  return jsonOrThrow<InvitationsListResponse>(await apiFetch(path));
}

export async function issueInvitation(body: IssueInviteBody): Promise<IssueInviteResponse> {
  return jsonOrThrow(
    await apiFetch("/api/admin/invitations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
}

export async function revokeInvitation(invitationId: string): Promise<{ ok: boolean }> {
  return jsonOrThrow(
    await apiFetch(`/api/admin/invitations/${encodeURIComponent(invitationId)}/revoke`, {
      method: "POST",
    }),
  );
}

// ─── Audit log ────────────────────────────────────────────────────────────

export type AuditEvent = {
  action_id: string;
  action_type: string;
  actor_email: string | null;
  subject_email: string | null;
  metadata: Record<string, unknown>;
  ip_address: string | null;
  created_at: string | null;
};

export async function fetchAuditEvents(params: {
  action_type?: string;
  subject_email?: string;
  actor_email?: string;
  since?: string;
  limit?: number;
} = {}): Promise<{ events: AuditEvent[]; total: number }> {
  const q = new URLSearchParams();
  if (params.action_type) q.set("action_type", params.action_type);
  if (params.subject_email) q.set("subject_email", params.subject_email);
  if (params.actor_email) q.set("actor_email", params.actor_email);
  if (params.since) q.set("since", params.since);
  if (params.limit) q.set("limit", String(params.limit));
  const path = `/api/admin/audit${q.toString() ? `?${q}` : ""}`;
  return jsonOrThrow(await apiFetch(path));
}

// ─── User-facing change password (NOT admin-gated) ────────────────────────

export async function changePassword(
  newPassword: string,
  currentPassword?: string,
): Promise<{ ok: boolean }> {
  return jsonOrThrow(
    await apiFetch("/api/auth/change-password", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        new_password: newPassword,
        ...(currentPassword ? { current_password: currentPassword } : {}),
      }),
    }),
  );
}

import { apiFetch } from "../lib/api-fetch";

export type AccountProfile = {
  user_id: string;
  email: string;
  first_name: string | null;
  last_name: string | null;
  firm: string | null;
  role: string | null;
  is_admin: boolean;
  created_at: string | null;
  last_login: string | null;
  invited_by: string | null;
  password_is_temporary: boolean;
};

async function jsonOrThrow<T>(res: Response): Promise<T> {
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    let detail = text;
    try {
      const parsed = JSON.parse(text) as { detail?: unknown };
      if (typeof parsed?.detail === "string") detail = parsed.detail;
    } catch {
      /* fall through to raw text */
    }
    throw new Error(detail || `API ${res.status}`);
  }
  return (await res.json()) as T;
}

export async function fetchAccount(): Promise<AccountProfile> {
  return jsonOrThrow<AccountProfile>(await apiFetch("/api/account"));
}

export async function updateProfile(body: {
  first_name: string;
  last_name: string;
  firm: string | null;
}): Promise<AccountProfile> {
  return jsonOrThrow<AccountProfile>(
    await apiFetch("/api/account", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
}

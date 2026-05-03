/**
 * frontend/app/lib/api-fetch.ts
 * =============================
 * Thin fetch wrapper that:
 *   - prepends NEXT_PUBLIC_API_BASE to relative paths,
 *   - sends credentials by default,
 *   - on 401, redirects the browser to /auth/signin?next=<current-path>.
 *
 * Migration note: existing fetch sites in the codebase still call native
 * fetch directly and handle 401 per-page. New code should prefer apiFetch.
 * Future cleanup pass can migrate the rest to use this helper consistently.
 */

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

export async function apiFetch(
  path: string,
  init: RequestInit = {},
): Promise<Response> {
  const url = path.startsWith("http") ? path : API_BASE + path;
  const res = await fetch(url, {
    credentials: "include",
    ...init,
    headers: {
      ...(init.headers || {}),
    },
  });

  if (res.status === 401 && typeof window !== "undefined") {
    // Avoid an infinite loop if /api/auth/me itself returns 401 from the
    // sign-in page — only redirect from non-auth routes.
    const onAuthRoute = window.location.pathname.startsWith("/auth/");
    if (!onAuthRoute) {
      const next = encodeURIComponent(
        window.location.pathname + window.location.search,
      );
      window.location.href = "/auth/signin?next=" + next;
    }
  }

  return res;
}

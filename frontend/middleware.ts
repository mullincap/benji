/**
 * frontend/middleware.ts
 * ======================
 * Next.js middleware runs before every request matching the `config.matcher`
 * paths. We do TWO things here:
 *
 *   (a) Bounce already-authenticated users away from /auth/signin and
 *       /auth/invite — no point making them re-enter credentials.
 *
 *   (b) Bounce un-authenticated users away from /admin/* — they have no
 *       business there. Note: the is_admin = true check happens at the
 *       admin layout level, NOT here. Middleware can only see the
 *       cookie's presence, not the user's flags. Doing the is_admin
 *       check here would require either a DB lookup (expensive on every
 *       admin request) or a parallel cookie. Layout-level is sufficient
 *       and matches the existing /trader/(protected) pattern.
 *
 * Auth check is cookie-presence only. Validity is verified server-side
 * on every API call; if the cookie is stale or tampered with, the user
 * lands on / and their first authenticated request will 401 → redirect
 * back to /auth/signin.
 *
 * /auth/welcome is intentionally NOT in the matcher: that page's own
 * mount-effect handles the first_login=true vs false branch.
 *
 * /auth/forgot is NOT in the matcher: it's a static stub that's safe
 * to see authenticated.
 */

import { NextResponse, type NextRequest } from "next/server";

const COOKIE_NAME = "user_session";

export function middleware(request: NextRequest) {
  const session = request.cookies.get(COOKIE_NAME);
  const path = request.nextUrl.pathname;

  // Admin gate — kick unauthenticated users to signin with a next-param.
  if (path === "/admin" || path.startsWith("/admin/")) {
    if (!session) {
      const url = request.nextUrl.clone();
      url.pathname = "/auth/signin";
      url.search = "?next=" + encodeURIComponent(path);
      return NextResponse.redirect(url);
    }
    return NextResponse.next();
  }

  // /auth/signin and /auth/invite — bounce already-authenticated users.
  if (session) {
    const url = request.nextUrl.clone();
    url.pathname = "/";
    url.search = "";
    return NextResponse.redirect(url);
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    "/auth/signin",
    "/auth/invite",
    "/auth/invite/:path*",
    "/admin",
    "/admin/:path*",
  ],
};

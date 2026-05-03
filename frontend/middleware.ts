/**
 * frontend/middleware.ts
 * ======================
 * Next.js middleware runs before every request matching the `config.matcher`
 * paths. We use it to bounce already-authenticated users out of the sign-in
 * and accept-invite screens — no point making them re-enter credentials.
 *
 * Auth check is cookie-presence only (the `user_session` cookie). Validity
 * is verified server-side on every API call; if the cookie is stale or
 * tampered with, the user lands on / and their first authenticated request
 * will 401 → redirect back to /auth/signin.
 *
 * /auth/welcome is intentionally NOT in the matcher: that page's own
 * mount-effect handles the first_login=true vs false branch (the cookie
 * presence alone doesn't tell us which). Hoisting that to middleware would
 * require either a DB lookup (expensive) or a separate first_login cookie.
 *
 * /auth/forgot is NOT in the matcher: it's a static stub that's safe to
 * see authenticated.
 */

import { NextResponse, type NextRequest } from "next/server";

const COOKIE_NAME = "user_session";

export function middleware(request: NextRequest) {
  const session = request.cookies.get(COOKIE_NAME);
  if (!session) return NextResponse.next();

  const url = request.nextUrl.clone();
  url.pathname = "/";
  url.search = "";
  return NextResponse.redirect(url);
}

export const config = {
  matcher: ["/auth/signin", "/auth/invite", "/auth/invite/:path*"],
};

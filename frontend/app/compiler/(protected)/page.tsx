/**
 * frontend/app/compiler/(protected)/page.tsx
 * ==========================================
 * Index of the protected compiler tree. Redirects to /compiler/coverage.
 *
 * This is a server component (no 'use client' directive) so the redirect
 * happens during the initial response — no client-side flicker. The
 * protected layout's auth check still runs after this redirect lands the
 * user on /compiler/coverage, so the user is never able to bypass auth
 * by going to /compiler directly.
 */

import { redirect } from "next/navigation";

export default function CompilerIndexPage() {
  redirect("/compiler/coverage");
}

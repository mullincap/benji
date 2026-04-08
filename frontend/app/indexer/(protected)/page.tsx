/**
 * frontend/app/indexer/(protected)/page.tsx
 * =========================================
 * Index of the protected indexer tree. Redirects to /indexer/coverage.
 *
 * Server component (no 'use client' directive) so the redirect happens
 * during the initial response — no client-side flicker. The protected
 * layout's auth check still runs after the redirect lands the user on
 * /indexer/coverage, so the user can never bypass auth by going to
 * /indexer directly.
 */

import { redirect } from "next/navigation";

export default function IndexerIndexPage() {
  redirect("/indexer/coverage");
}

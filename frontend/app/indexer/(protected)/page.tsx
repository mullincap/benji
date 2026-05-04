/**
 * frontend/app/indexer/(protected)/page.tsx
 * =========================================
 * Index of the protected indexer tree. Redirects to /indexer/signals
 * (the primary working surface — Coverage is a reference view).
 *
 * Server component (no 'use client' directive) so the redirect happens
 * during the initial response — no client-side flicker. The protected
 * layout's auth check still runs after the redirect lands the user on
 * /indexer/signals, so the user can never bypass auth by going to
 * /indexer directly.
 */

import { redirect } from "next/navigation";

export default function IndexerIndexPage() {
  redirect("/indexer/signals");
}

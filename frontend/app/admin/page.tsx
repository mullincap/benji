"use client";

/**
 * frontend/app/admin/page.tsx
 * ============================
 * Admin Console index. Lands at /admin and redirects to /admin/users
 * (the canonical entry point per the mockup).
 *
 * Phase 1 commit 3 ships only the redirect; commit 4 wires up the
 * actual /admin/users screen.
 */

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function AdminIndexPage() {
  const router = useRouter();
  useEffect(() => {
    router.replace("/admin/users");
  }, [router]);
  return null;
}

import type { Metadata } from "next";

// The dynamic [user_id] segment is a UUID — not useful as a tab title.
// Static "Admin · User" is the cleanest fallback. Could fetch email
// server-side later if useful (admin tab navigation across multiple
// users would benefit from "Admin · alice@example.com" etc.).
export const metadata: Metadata = { title: "Admin · User" };

export default function AdminUserDetailLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

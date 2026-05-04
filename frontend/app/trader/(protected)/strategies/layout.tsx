import type { Metadata } from "next";

// Trader-facing user copy uses "fund" terminology (UI rebrand). The
// underlying data model + URL path + admin module all stay
// "strategy" — the rename is at the rendering boundary only.
export const metadata: Metadata = { title: "Funds" };

export default function TitleLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

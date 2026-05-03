import type { Metadata } from "next";

// The dynamic [id] segment here is an internal trader/instance id —
// not a meaningful tab-title string. Static "Trader" fallback is
// cleaner than rendering the raw id. Refine to the strategy name +
// exchange if/when that's worth a server-side fetch.
export const metadata: Metadata = { title: "Trader" };

export default function TraderDetailLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

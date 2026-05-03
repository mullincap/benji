import type { Metadata } from "next";

export const metadata: Metadata = { title: "Strategies" };

export default function TitleLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

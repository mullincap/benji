import type { Metadata } from "next";

export const metadata: Metadata = { title: "Manager" };

export default function TitleLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

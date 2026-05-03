import type { Metadata } from "next";

export const metadata: Metadata = { title: "Admin · Audit" };

export default function TitleLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

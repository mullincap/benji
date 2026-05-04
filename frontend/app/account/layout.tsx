import type { Metadata } from "next";
import AccountShell from "./_AccountShell";

export const metadata: Metadata = { title: "Account" };

export default function AccountLayout({ children }: { children: React.ReactNode }) {
  return <AccountShell>{children}</AccountShell>;
}

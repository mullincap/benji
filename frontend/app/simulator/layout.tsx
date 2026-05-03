import type { Metadata } from "next";

export const metadata: Metadata = { title: "Simulator" };

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="h-full overflow-hidden">
      {children}
    </div>
  );
}

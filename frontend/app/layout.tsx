import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Benji3m — Audit",
  description: "Risk Audit Engine",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full">
      <body className="h-full overflow-hidden">{children}</body>
    </html>
  );
}

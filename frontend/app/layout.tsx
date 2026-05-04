import type { Metadata } from "next";
import "@fontsource/space-mono/400.css";
import "@fontsource/space-mono/700.css";
import "./globals.css";
import { AuthProvider } from "./lib/auth";
import { ConfirmProvider } from "./components/ConfirmDialog";
import TempPasswordBanner from "./components/TempPasswordBanner";

export const metadata: Metadata = {
  // template applies to every child route that exports `title: "..."` —
  // resolves to "3M — {Page Name}" in the browser tab. Routes that don't
  // set a title (or the marketing landing /) fall through to `default`.
  title: {
    default: "3M",
    template: "3M — %s",
  },
  description: "Quantitative risk audit engine for crypto fund managers and allocators",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full">
      <body className="h-full">
        <AuthProvider>
          <ConfirmProvider>
            <TempPasswordBanner />
            {children}
          </ConfirmProvider>
        </AuthProvider>
      </body>
    </html>
  );
}

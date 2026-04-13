/**
 * frontend/app/trader/layout.tsx
 * ==============================
 * Root layout for /trader/*. Pass-through only — the actual chrome lives in
 * (protected)/layout.tsx (auth guard + sidebar) and (public)/layout.tsx
 * (login page wrapper). Route group parens don't appear in URLs.
 */

export default function TraderRootLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

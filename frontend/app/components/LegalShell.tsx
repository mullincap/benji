import Link from 'next/link';
import type { ReactNode } from 'react';

const navStyle: React.CSSProperties = {
  position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100,
  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
  padding: '0 4rem', height: 64,
  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
  background: 'rgba(8, 8, 9, 0.72)',
  backdropFilter: 'blur(12px)',
  WebkitBackdropFilter: 'blur(12px)',
  borderBottom: '1px solid var(--line)',
};

const logoStyle: React.CSSProperties = {
  fontSize: 13, fontWeight: 700, color: 'var(--t0)', textDecoration: 'none',
  letterSpacing: '0.1em', textTransform: 'uppercase',
  display: 'flex', alignItems: 'center', gap: 10,
};

const ctaStyle: React.CSSProperties = {
  padding: '9px 22px', background: 'transparent',
  border: '1px solid var(--line2)', color: 'var(--t1)',
  fontFamily: 'var(--font-space-mono)', fontSize: 11,
  letterSpacing: '0.1em', textTransform: 'uppercase', textDecoration: 'none',
};

const footerStyle: React.CSSProperties = {
  borderTop: '1px solid var(--line)',
  padding: '2rem 4rem',
  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
  gap: '1rem', flexWrap: 'wrap',
  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
};

const footerLinkStyle: React.CSSProperties = {
  fontSize: 10, color: 'var(--t3)', textDecoration: 'none',
  letterSpacing: '0.08em', textTransform: 'uppercase',
};

export function LegalShell({
  title,
  effective,
  children,
}: {
  title: string;
  effective: string;
  children: ReactNode;
}) {
  return (
    <>
      <nav style={navStyle}>
        <Link href="/" style={logoStyle}>
          <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--green)', flexShrink: 0 }} />
          3M
        </Link>
        <Link href="/trader/overview" style={ctaStyle}>
          Launch App
        </Link>
      </nav>

      <main style={{
        maxWidth: 760, margin: '0 auto',
        padding: '140px 2rem 100px',
        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
      }}>
        <div style={{
          fontSize: 9, letterSpacing: '0.18em', textTransform: 'uppercase',
          color: 'var(--t3)', fontWeight: 700, marginBottom: '1.5rem',
          display: 'flex', alignItems: 'center', gap: 12,
        }}>
          <span style={{ width: 20, height: 1, background: 'var(--t3)', flexShrink: 0 }} />
          Legal
        </div>
        <h1 style={{
          fontSize: 'clamp(28px, 3.5vw, 42px)', fontWeight: 700,
          color: 'var(--t0)', marginBottom: '0.5rem', letterSpacing: -0.5,
          lineHeight: 1.1,
        }}>
          {title}
        </h1>
        <p style={{
          fontSize: 10, color: 'var(--t3)', letterSpacing: '0.08em',
          textTransform: 'uppercase', marginBottom: '3rem',
        }}>
          Effective {effective}
        </p>
        <div style={{ fontSize: 12, color: 'var(--t1)', lineHeight: 1.85 }}>
          {children}
        </div>
      </main>

      <footer style={footerStyle}>
        <Link href="/" style={logoStyle}>
          <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--green)', flexShrink: 0 }} />
          3M
        </Link>
        <p style={{ fontSize: 10, color: 'var(--t3)', letterSpacing: '0.04em' }}>
          © {new Date().getFullYear()} Mullin Capital. All rights reserved.
        </p>
        <ul style={{ display: 'flex', gap: '2rem', listStyle: 'none' }}>
          <li><Link href="/privacy" style={footerLinkStyle}>Privacy</Link></li>
          <li><Link href="/terms" style={footerLinkStyle}>Terms</Link></li>
          <li><a href="mailto:j@mullincap.com" style={footerLinkStyle}>Contact</a></li>
          <li><Link href="/trader/overview" style={footerLinkStyle}>App</Link></li>
        </ul>
      </footer>
    </>
  );
}

export function LegalSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section style={{ marginBottom: '2.5rem' }}>
      <h2 style={{
        fontSize: 11, fontWeight: 700, color: 'var(--t0)',
        letterSpacing: '0.14em', textTransform: 'uppercase',
        marginBottom: '1rem',
      }}>
        {title}
      </h2>
      {children}
    </section>
  );
}

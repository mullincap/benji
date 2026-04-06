'use client';

import Link from 'next/link';
import { WaveAnimation } from './components/ui/wave-animation';

export default function LandingPage() {
  return (
    <div style={{
      minHeight: '100vh',
      background: '#0a0a0a',
      color: '#e0e0e0',
      position: 'relative',
      overflow: 'hidden',
      fontFamily: 'var(--font-space-mono), Space Mono, monospace',
    }}>
      {/* Three.js wave background */}
      <div style={{ position: 'fixed', top: 0, left: 0, width: '100%', height: '100%', pointerEvents: 'none' }}>
        <WaveAnimation
          waveSpeed={0.8}
          waveIntensity={1.2}
          particleColor="#3cff78"
          pointSize={2.0}
          gridDistance={3}
        />
      </div>

      {/* Nav */}
      <nav style={{
        position: 'relative',
        zIndex: 10,
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '24px 40px',
      }}>
        <span style={{ fontSize: 18, fontWeight: 700, letterSpacing: '0.08em', color: '#fff' }}>
          3M
        </span>
        <Link
          href="/simulator"
          style={{
            fontSize: 11,
            letterSpacing: '0.1em',
            textTransform: 'uppercase',
            color: '#0a0a0a',
            background: 'rgba(60, 255, 120, 0.85)',
            padding: '8px 20px',
            borderRadius: 4,
            textDecoration: 'none',
            fontWeight: 600,
            transition: 'background 0.2s',
          }}
          onMouseEnter={e => (e.currentTarget.style.background = 'rgba(60, 255, 120, 1)')}
          onMouseLeave={e => (e.currentTarget.style.background = 'rgba(60, 255, 120, 0.85)')}
        >
          Launch App
        </Link>
      </nav>

      {/* Hero */}
      <main style={{
        position: 'relative',
        zIndex: 10,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        textAlign: 'center',
        minHeight: 'calc(100vh - 200px)',
        padding: '0 24px',
      }}>
        <div style={{
          fontSize: 11,
          letterSpacing: '0.2em',
          textTransform: 'uppercase',
          color: 'rgba(60, 255, 120, 0.7)',
          marginBottom: 24,
        }}>
          Institutional-Grade Risk Analytics
        </div>

        <h1 style={{
          fontSize: 'clamp(36px, 6vw, 72px)',
          fontWeight: 700,
          lineHeight: 1.1,
          color: '#fff',
          margin: '0 0 20px',
          maxWidth: 800,
          letterSpacing: '-0.02em',
        }}>
          Quantitative engine for{' '}
          <span style={{ color: 'rgba(60, 255, 120, 0.9)' }}>crypto strategies</span>
        </h1>

        <p style={{
          fontSize: 'clamp(14px, 1.8vw, 18px)',
          color: 'rgba(255,255,255,0.45)',
          maxWidth: 560,
          lineHeight: 1.6,
          margin: '0 0 40px',
        }}>
          Backtest, stress-test, and validate trading strategies with
          institutional-grade analytics. Built for fund managers and allocators.
        </p>

        <Link
          href="/simulator"
          style={{
            fontSize: 13,
            letterSpacing: '0.12em',
            textTransform: 'uppercase',
            color: '#0a0a0a',
            background: 'rgba(60, 255, 120, 0.85)',
            padding: '14px 40px',
            borderRadius: 5,
            textDecoration: 'none',
            fontWeight: 700,
            transition: 'all 0.2s',
            boxShadow: '0 0 30px rgba(60, 255, 120, 0.15)',
          }}
          onMouseEnter={e => {
            e.currentTarget.style.background = 'rgba(60, 255, 120, 1)';
            e.currentTarget.style.boxShadow = '0 0 40px rgba(60, 255, 120, 0.3)';
          }}
          onMouseLeave={e => {
            e.currentTarget.style.background = 'rgba(60, 255, 120, 0.85)';
            e.currentTarget.style.boxShadow = '0 0 30px rgba(60, 255, 120, 0.15)';
          }}
        >
          Launch Audit Engine
        </Link>

        {/* Feature pills */}
        <div style={{
          display: 'flex',
          gap: 12,
          marginTop: 60,
          flexWrap: 'wrap',
          justifyContent: 'center',
        }}>
          {[
            'Sharpe & DSR Validation',
            'Walk-Forward Testing',
            'Regime Robustness',
            'Parameter Sensitivity',
            'Stress Testing',
          ].map((feat) => (
            <span
              key={feat}
              style={{
                fontSize: 10,
                letterSpacing: '0.06em',
                color: 'rgba(255,255,255,0.6)',
                border: '1px solid rgba(255,255,255,0.12)',
                borderRadius: 20,
                padding: '6px 14px',
                background: 'rgba(0,0,0,0.4)',
                backdropFilter: 'blur(6px)',
              }}
            >
              {feat}
            </span>
          ))}
        </div>
      </main>

      {/* Footer */}
      <footer style={{
        position: 'relative',
        zIndex: 10,
        textAlign: 'center',
        padding: '20px 0 30px',
        fontSize: 10,
        color: 'rgba(255,255,255,0.2)',
        letterSpacing: '0.05em',
      }}>
        heybenji.io
      </footer>
    </div>
  );
}

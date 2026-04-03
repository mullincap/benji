'use client';

import { useEffect, useRef } from 'react';
import Link from 'next/link';

function WaveCanvas() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    let animationId: number;
    let time = 0;

    function resize() {
      if (!canvas) return;
      canvas.width = window.innerWidth * 2;
      canvas.height = window.innerHeight * 2;
      canvas.style.width = `${window.innerWidth}px`;
      canvas.style.height = `${window.innerHeight}px`;
    }

    resize();
    window.addEventListener('resize', resize);

    function draw() {
      if (!canvas || !ctx) return;
      const w = canvas.width;
      const h = canvas.height;

      ctx.clearRect(0, 0, w, h);

      const waves = [
        { amplitude: 40, wavelength: 600, speed: 0.015, y: h * 0.62, color: 'rgba(60, 255, 120, 0.06)', lineColor: 'rgba(60, 255, 120, 0.15)' },
        { amplitude: 30, wavelength: 450, speed: 0.02, y: h * 0.65, color: 'rgba(60, 255, 120, 0.04)', lineColor: 'rgba(60, 255, 120, 0.10)' },
        { amplitude: 50, wavelength: 800, speed: 0.01, y: h * 0.68, color: 'rgba(60, 255, 120, 0.03)', lineColor: 'rgba(60, 255, 120, 0.07)' },
        { amplitude: 25, wavelength: 350, speed: 0.025, y: h * 0.72, color: 'rgba(60, 255, 120, 0.02)', lineColor: 'rgba(60, 255, 120, 0.05)' },
      ];

      for (const wave of waves) {
        ctx.beginPath();
        ctx.moveTo(0, wave.y);

        for (let x = 0; x <= w; x += 4) {
          const y = wave.y +
            Math.sin((x / wave.wavelength) * Math.PI * 2 + time * wave.speed) * wave.amplitude +
            Math.sin((x / (wave.wavelength * 1.7)) * Math.PI * 2 - time * wave.speed * 0.7) * wave.amplitude * 0.5;
          ctx.lineTo(x, y);
        }

        ctx.lineTo(w, h);
        ctx.lineTo(0, h);
        ctx.closePath();
        ctx.fillStyle = wave.color;
        ctx.fill();

        ctx.beginPath();
        for (let x = 0; x <= w; x += 4) {
          const y = wave.y +
            Math.sin((x / wave.wavelength) * Math.PI * 2 + time * wave.speed) * wave.amplitude +
            Math.sin((x / (wave.wavelength * 1.7)) * Math.PI * 2 - time * wave.speed * 0.7) * wave.amplitude * 0.5;
          if (x === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.strokeStyle = wave.lineColor;
        ctx.lineWidth = 1;
        ctx.stroke();
      }

      time += 1;
      animationId = requestAnimationFrame(draw);
    }

    draw();

    return () => {
      cancelAnimationFrame(animationId);
      window.removeEventListener('resize', resize);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        width: '100%',
        height: '100%',
        pointerEvents: 'none',
      }}
    />
  );
}

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
      <WaveCanvas />

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
          BENJI
        </span>
        <Link
          href="/dashboard"
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
          Quantitative audit engine for{' '}
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
          href="/dashboard"
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
                color: 'rgba(255,255,255,0.35)',
                border: '1px solid rgba(255,255,255,0.08)',
                borderRadius: 20,
                padding: '6px 14px',
                background: 'rgba(255,255,255,0.02)',
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
        hellobenji.com
      </footer>
    </div>
  );
}

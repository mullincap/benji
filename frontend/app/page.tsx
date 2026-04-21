'use client';

import { useEffect, useRef, useState } from 'react';
import Link from 'next/link';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || '';

// ── Neural network canvas animation ─────────────────────────────────────────
function NeuralCanvas() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const mouseRef = useRef({ x: -999, y: -999 });

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const NODE_COUNT = 55;
    const MAX_DIST = 180;
    let W = 0, H = 0;
    let animId: number;

    const nodes: { x: number; y: number; vx: number; vy: number; r: number }[] = [];

    function resize() {
      W = canvas!.width = window.innerWidth;
      H = canvas!.height = window.innerHeight;
    }

    function init() {
      resize();
      nodes.length = 0;
      for (let i = 0; i < NODE_COUNT; i++) {
        nodes.push({
          x: Math.random() * W,
          y: Math.random() * H,
          vx: (Math.random() - 0.5) * 0.28,
          vy: (Math.random() - 0.5) * 0.28,
          r: Math.random() * 1.2 + 0.5,
        });
      }
    }

    function draw() {
      ctx!.clearRect(0, 0, W, H);
      const mouse = mouseRef.current;

      for (const n of nodes) {
        n.x += n.vx; n.y += n.vy;
        if (n.x < 0 || n.x > W) n.vx *= -1;
        if (n.y < 0 || n.y > H) n.vy *= -1;
      }

      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const dx = nodes[i].x - nodes[j].x;
          const dy = nodes[i].y - nodes[j].y;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d < MAX_DIST) {
            const alpha = (1 - d / MAX_DIST) * 0.06;
            ctx!.beginPath();
            ctx!.moveTo(nodes[i].x, nodes[i].y);
            ctx!.lineTo(nodes[j].x, nodes[j].y);
            ctx!.strokeStyle = `rgba(240,237,230,${alpha})`;
            ctx!.lineWidth = 0.5;
            ctx!.stroke();
          }
        }
      }

      for (const n of nodes) {
        const dx = n.x - mouse.x;
        const dy = n.y - mouse.y;
        const d = Math.sqrt(dx * dx + dy * dy);
        if (d < 140) {
          const alpha = (1 - d / 140) * 0.22;
          ctx!.beginPath();
          ctx!.moveTo(n.x, n.y);
          ctx!.lineTo(mouse.x, mouse.y);
          ctx!.strokeStyle = `rgba(0,200,150,${alpha})`;
          ctx!.lineWidth = 0.8;
          ctx!.stroke();
        }
      }

      for (const n of nodes) {
        ctx!.beginPath();
        ctx!.arc(n.x, n.y, n.r, 0, Math.PI * 2);
        ctx!.fillStyle = 'rgba(240,237,230,0.18)';
        ctx!.fill();
      }

      animId = requestAnimationFrame(draw);
    }

    const handleResize = () => resize();
    const handleMouseMove = (e: MouseEvent) => { mouseRef.current = { x: e.clientX, y: e.clientY }; };
    const handleMouseLeave = () => { mouseRef.current = { x: -999, y: -999 }; };

    window.addEventListener('resize', handleResize);
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseleave', handleMouseLeave);

    init();
    draw();

    return () => {
      cancelAnimationFrame(animId);
      window.removeEventListener('resize', handleResize);
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseleave', handleMouseLeave);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      style={{ position: 'fixed', inset: 0, zIndex: 0, pointerEvents: 'none' }}
    />
  );
}

// ── Landing page ─────────────────────────────────────────────────────────────
export default function LandingPage() {
  const [wlEmail, setWlEmail] = useState('');
  const [wlMsg, setWlMsg] = useState('No spam. Unsubscribe at any time.');
  const [wlMsgColor, setWlMsgColor] = useState('var(--t3)');
  const [wlSubmitted, setWlSubmitted] = useState(false);

  async function handleWL() {
    if (!wlEmail || !wlEmail.includes('@')) {
      setWlMsg('Please enter a valid email address.');
      setWlMsgColor('var(--t1)');
      return;
    }
    try {
      const res = await fetch(API_BASE + '/api/waitlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: wlEmail }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        setWlMsg(body.detail || 'Could not submit. Please try again.');
        setWlMsgColor('var(--t1)');
        return;
      }
      setWlMsg("✓ You're on the list. We'll be in touch.");
      setWlMsgColor('var(--green)');
      setWlEmail('');
      setWlSubmitted(true);
    } catch {
      setWlMsg('Network error. Please try again.');
      setWlMsgColor('var(--t1)');
    }
  }

  return (
    <>
      <style>{`
        @keyframes breathe {
          0%,100% { opacity: 1; transform: scale(1); box-shadow: 0 0 0 0 rgba(0,200,150,0.4); }
          50% { opacity: 0.7; transform: scale(0.85); box-shadow: 0 0 0 4px rgba(0,200,150,0); }
        }
        @keyframes rise {
          from { opacity: 0; transform: translateY(20px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        @keyframes scan-reveal {
          to { clip-path: inset(0 0% 0 0); }
        }
        .lp-rise-1 { opacity: 0; animation: rise 1s cubic-bezier(0.16,1,0.3,1) 0.1s forwards; }
        .lp-rise-2 { opacity: 0; animation: rise 1s cubic-bezier(0.16,1,0.3,1) 0.2s forwards; }
        .lp-rise-3 { opacity: 0; animation: rise 1s cubic-bezier(0.16,1,0.3,1) 0.35s forwards; }
        .lp-rise-4 { opacity: 0; animation: rise 1s cubic-bezier(0.16,1,0.3,1) 0.45s forwards; }
        .lp-rise-5 { opacity: 0; animation: rise 1s cubic-bezier(0.16,1,0.3,1) 0.55s forwards; }
        .lp-word-ai {
          position: relative;
          color: transparent;
          -webkit-text-stroke: 1px rgba(240,237,230,0.25);
          display: inline-block;
        }
        .lp-word-ai::after {
          content: attr(data-text);
          position: absolute;
          inset: 0;
          color: var(--t0);
          -webkit-text-stroke: 0;
          clip-path: inset(0 100% 0 0);
          animation: scan-reveal 2.5s cubic-bezier(0.16,1,0.3,1) 1.2s forwards;
        }
        .lp-breathe { animation: breathe 3s ease-in-out infinite; }
        .lp-breathe-fast { animation: breathe 2s ease-in-out infinite; }
        .lp-mod-card { transition: background 0.4s cubic-bezier(0.16,1,0.3,1); }
        .lp-mod-card:hover { background: var(--bg2) !important; }
        .lp-step:hover h3 { color: var(--t0) !important; }
        .lp-step:hover p { color: var(--t2) !important; }
        .lp-step:hover .lp-step-num { color: var(--green) !important; }
        .lp-step:hover .lp-tag { color: var(--t2) !important; }
        .lp-step:hover .lp-mod-li { color: var(--t2) !important; }
        .lp-mod-card:hover .lp-mod-li { color: var(--t2) !important; }
        .lp-ap-row:hover { background: var(--bg2); }
        .lp-pricing-card { transition: background 0.3s; }
        .lp-pricing-card:hover { background: var(--bg2) !important; }
        .lp-nav-link { transition: color 0.3s; }
        .lp-nav-link:hover { color: var(--t0) !important; }
        .lp-nav-cta { transition: border-color 0.3s, color 0.3s; }
        .lp-nav-cta:hover { border-color: var(--green) !important; color: var(--green) !important; }
        .lp-btn-primary { transition: opacity 0.2s, transform 0.2s; }
        .lp-btn-primary:hover { opacity: 0.85; transform: translateY(-1px); }
        .lp-btn-ghost { transition: border-color 0.2s, color 0.2s; }
        .lp-btn-ghost:hover { border-color: var(--t1) !important; color: var(--t0) !important; }
        .lp-footer-link { transition: color 0.2s; }
        .lp-footer-link:hover { color: var(--t0) !important; }
        .lp-p-cta { transition: all 0.3s; }
        .lp-p-cta:hover { border-color: var(--t1) !important; color: var(--t0) !important; }
        .lp-p-cta-featured { transition: opacity 0.3s; }
        .lp-p-cta-featured:hover { opacity: 0.85; }
        @media (max-width: 1100px) {
          .lp-modules-grid { grid-template-columns: repeat(3,1fr) !important; }
          .lp-how-layout { grid-template-columns: 1fr !important; gap: 4rem !important; }
          .lp-metrics-split { grid-template-columns: 1fr !important; gap: 4rem !important; }
          .lp-pricing-grid { grid-template-columns: 1fr !important; }
        }
        @media (max-width: 700px) {
          .lp-section { padding: 80px 1.5rem !important; }
          .lp-modules-grid { grid-template-columns: 1fr 1fr !important; }
          .lp-hero-actions { flex-direction: column !important; width: 100%; align-items: stretch; }
          .lp-hero-actions > a { text-align: center; }
          .lp-nav { padding: 0 1.25rem !important; }
          .lp-nav-links { display: none !important; }
          .lp-hero { padding: 110px 1.25rem 70px !important; }
          .lp-hero-stats { flex-wrap: wrap !important; }
          .lp-hero-stats > div {
            flex: 1 0 33.333% !important;
            border-right: 1px solid var(--line) !important;
            border-bottom: 1px solid var(--line);
            padding: 18px 0 !important;
          }
          .lp-hero-stats > div:nth-child(3n) { border-right: none !important; }
          .lp-hero-stats > div:nth-last-child(-n+2) { border-bottom: none; }
          .lp-waitlist { padding: 100px 1.25rem !important; }
          .lp-footer { padding: 2rem 1.25rem !important; }
          .lp-footer-links { gap: 1.25rem !important; flex-wrap: wrap; justify-content: center; }
        }
        @media (max-width: 480px) {
          .lp-modules-grid { grid-template-columns: 1fr !important; }
          .lp-waitlist-form { flex-direction: column !important; gap: 8px !important; }
          .lp-waitlist-form > input { border-right: 1px solid var(--line2) !important; }
          .lp-waitlist-form > button { padding: 14px !important; }
        }
      `}</style>

      <NeuralCanvas />

      {/* Nav */}
      <nav className="lp-nav" style={{
        position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100,
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '0 4rem', height: 64,
        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
        background: 'rgba(8, 8, 9, 0.72)',
        backdropFilter: 'blur(12px)',
        WebkitBackdropFilter: 'blur(12px)',
        borderBottom: '1px solid var(--line)',
      }}>
        <a href="#" style={{
          fontSize: 13, fontWeight: 700, color: 'var(--t0)', textDecoration: 'none',
          letterSpacing: '0.1em', textTransform: 'uppercase',
          display: 'flex', alignItems: 'center', gap: 10,
        }}>
          <span className="lp-breathe" style={{
            width: 6, height: 6, borderRadius: '50%', background: 'var(--green)', flexShrink: 0,
          }} />
          3M
        </a>
        <ul className="lp-nav-links" style={{ display: 'flex', alignItems: 'center', gap: '2.5rem', listStyle: 'none' }}>
          {['Modules', 'Process', 'Metrics', 'Pricing'].map((label) => (
            <li key={label}>
              <a href={`#${label.toLowerCase()}`} className="lp-nav-link" style={{
                textDecoration: 'none', fontSize: 11, color: 'var(--t2)',
                letterSpacing: '0.08em', textTransform: 'uppercase',
              }}>{label}</a>
            </li>
          ))}
        </ul>
        <Link href="/trader/overview" className="lp-nav-cta" style={{
          padding: '9px 22px', background: 'transparent',
          border: '1px solid var(--line2)', color: 'var(--t1)',
          fontFamily: 'var(--font-space-mono)', fontSize: 11,
          letterSpacing: '0.1em', textTransform: 'uppercase', textDecoration: 'none',
        }}>
          Launch App
        </Link>
      </nav>

      {/* Hero */}
      <section className="lp-hero" style={{
        position: 'relative', zIndex: 1, minHeight: '100vh',
        display: 'grid', placeItems: 'center',
        padding: '120px 4rem 80px', textAlign: 'center',
      }}>
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', maxWidth: 820 }}>
          <div className="lp-rise-1" style={{
            display: 'inline-flex', alignItems: 'center', gap: 8,
            padding: '6px 16px', border: '1px solid var(--line2)',
            fontSize: 10, letterSpacing: '0.14em', textTransform: 'uppercase',
            color: 'var(--t2)', marginBottom: '3rem',
          }}>
            <span className="lp-breathe-fast" style={{
              width: 4, height: 4, borderRadius: '50%', background: 'var(--green)',
            }} />
            Institutional-grade risk analytics
          </div>

          <h1 className="lp-rise-2" style={{
            fontSize: 'clamp(36px, 5.5vw, 76px)', fontWeight: 700, lineHeight: 1.0,
            letterSpacing: -1.5, color: 'var(--t0)', marginBottom: '2rem',
          }}>
            Quant engine for{' '}
            <span className="lp-word-ai" data-text="crypto strategies">crypto strategies</span>
          </h1>

          <p className="lp-rise-3" style={{
            fontSize: 13, color: 'var(--t1)', maxWidth: 480, lineHeight: 1.9, marginBottom: '3rem',
          }}>
            Backtest, stress-test, validate, and launch trading strategies with institutional-grade analytics
            linked directly to your account. Built for fund managers and allocators who demand rigor.
          </p>

          <div className="lp-rise-4 lp-hero-actions" style={{ display: 'flex', gap: 12, marginBottom: '5rem' }}>
            <Link href="/trader/overview" className="lp-btn-primary" style={{
              padding: '13px 32px', background: 'var(--green)', color: '#000',
              fontFamily: 'var(--font-space-mono)', fontWeight: 700,
              fontSize: 11, letterSpacing: '0.1em', textTransform: 'uppercase', textDecoration: 'none',
            }}>
              Launch App
            </Link>
            <a href="#modules" className="lp-btn-ghost" style={{
              padding: '13px 32px', border: '1px solid var(--line2)',
              color: 'var(--t1)', fontFamily: 'var(--font-space-mono)',
              fontSize: 11, letterSpacing: '0.1em', textTransform: 'uppercase', textDecoration: 'none',
            }}>
              Explore Modules
            </a>
          </div>

          <div className="lp-rise-5 lp-hero-stats" style={{
            display: 'flex', gap: 0, borderTop: '1px solid var(--line)',
            width: '100%', maxWidth: 680,
          }}>
            {[
              { val: '3.52', cls: 'g', label: 'Sharpe' },
              { val: '10.41', cls: '', label: 'Sortino' },
              { val: '−29.2%', cls: 'r', label: 'Max DD' },
              { val: '85.7%', cls: '', label: 'Win Rate' },
              { val: '69.50', cls: '', label: 'Calmar' },
            ].map((m, i, arr) => (
              <div key={m.label} style={{
                flex: 1, padding: '24px 0',
                borderRight: i < arr.length - 1 ? '1px solid var(--line)' : 'none',
                textAlign: 'center',
              }}>
                <span style={{
                  fontSize: 22, fontWeight: 700, letterSpacing: -0.5, marginBottom: 4, display: 'block',
                  color: m.cls === 'g' ? 'var(--green)' : m.cls === 'r' ? 'var(--t1)' : 'var(--t0)',
                }}>{m.val}</span>
                <span style={{
                  fontSize: 9, color: 'var(--t3)', letterSpacing: '0.12em',
                  textTransform: 'uppercase', fontWeight: 700, display: 'block',
                }}>{m.label}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <div style={{ width: '100%', height: 1, background: 'var(--line)', position: 'relative', zIndex: 1 }} />

      {/* Modules */}
      <div className="lp-section" id="modules" style={{
        position: 'relative', zIndex: 1, padding: '120px 4rem',
        maxWidth: 1200, margin: '0 auto', borderTop: '1px solid var(--line)',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', gap: '3rem', marginBottom: '5rem', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: 9, letterSpacing: '0.18em', textTransform: 'uppercase', color: 'var(--t3)', fontWeight: 700, marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: 12 }}>
              <span style={{ width: 20, height: 1, background: 'var(--t3)', flexShrink: 0 }} />
              Platform Architecture
            </div>
            <h2 style={{ fontSize: 'clamp(28px, 3.5vw, 48px)', fontWeight: 700, lineHeight: 1.08, letterSpacing: -0.8, color: 'var(--t0)', marginBottom: '1.25rem' }}>
              Five modules.<br />One unified system.
            </h2>
          </div>
          <p style={{ fontSize: 12, color: 'var(--t1)', lineHeight: 1.9, maxWidth: 440 }}>
            Each module is purpose-built for a distinct stage of the quantitative investment process — from raw data ingestion to live portfolio oversight.
          </p>
        </div>

        <div className="lp-modules-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 1, background: 'var(--line)' }}>
          {[
            { key: 'compiler', n: '01', name: 'Compiler', color: '#00C2FF', desc: 'Ingests, normalizes, and structures raw market data across exchanges, timeframes, and asset classes into a unified format.', items: ['Multi-exchange data ingestion', 'OHLCV normalization', 'On-chain data integration', 'Real-time feed support'] },
            { key: 'indexer', n: '02', name: 'Indexer', color: '#00E5C8', desc: 'Builds and maintains custom indices and universes. Screens assets by liquidity, volatility, and correlation.', items: ['Custom universe construction', 'Liquidity screening', 'Correlation clustering', 'Dynamic rebalancing logic'] },
            { key: 'simulator', n: '03', name: 'Simulator', color: '#00c896', desc: 'Core audit engine. Walk-forward backtests, Monte Carlo stress tests, and regime-conditional simulations.', items: ['Walk-forward validation', 'Monte Carlo simulation', 'Sharpe & DSR validation', 'Regime robustness testing'] },
            { key: 'allocator', n: '04', name: 'Allocator', color: '#A78BFF', desc: 'Optimizes capital allocation using mean-variance, risk parity, and Black-Litterman frameworks with drawdown constraints.', items: ['Mean-variance optimization', 'Risk parity weighting', 'Drawdown-aware sizing', 'Kelly criterion integration'] },
            { key: 'manager', n: '05', name: 'Manager', color: '#7B5FFF', desc: 'Live portfolio monitoring. Tracks P&L attribution, risk exposures, and drift against target allocations.', items: ['Real-time P&L attribution', 'Risk exposure monitoring', 'Allocation drift alerts', 'Execution analytics'] },
          ].map((mod) => (
            <div key={mod.key} className="lp-mod-card" style={{ background: 'var(--bg0)', padding: '2.5rem 2rem', position: 'relative', overflow: 'hidden', cursor: 'default' }}>
              <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.14em', textTransform: 'uppercase', color: mod.color, marginBottom: '2rem' }}>
                {mod.n} — {mod.name}
              </div>
              <div style={{ fontSize: 15, fontWeight: 700, color: 'var(--t0)', marginBottom: '0.75rem' }}>{mod.name}</div>
              <p style={{ fontSize: 11, color: 'var(--t2)', lineHeight: 1.75, marginBottom: '1.5rem' }}>{mod.desc}</p>
              <ul style={{ listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 6 }}>
                {mod.items.map((item) => (
                  <li key={item} className="lp-mod-li" style={{ fontSize: 10, color: 'var(--t3)', paddingLeft: 12, position: 'relative', letterSpacing: '0.03em', transition: 'color 0.2s' }}>
                    <span style={{ position: 'absolute', left: 0, color: mod.color, opacity: 0.5 }}>→</span>
                    {item}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>

      <div style={{ width: '100%', height: 1, background: 'var(--line)', position: 'relative', zIndex: 1 }} />

      {/* How It Works */}
      <div className="lp-section" id="process" style={{
        position: 'relative', zIndex: 1, padding: '120px 4rem',
        maxWidth: 1200, margin: '0 auto', borderTop: '1px solid var(--line)',
      }}>
        <div style={{ fontSize: 9, letterSpacing: '0.18em', textTransform: 'uppercase', color: 'var(--t3)', fontWeight: 700, marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ width: 20, height: 1, background: 'var(--t3)', flexShrink: 0 }} />
          Process
        </div>
        <h2 style={{ fontSize: 'clamp(28px, 3.5vw, 48px)', fontWeight: 700, lineHeight: 1.08, letterSpacing: -0.8, color: 'var(--t0)', marginBottom: '1.25rem' }}>
          From raw data to<br />validated strategy.
        </h2>

        <div className="lp-how-layout" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8rem', alignItems: 'start', marginTop: '5rem' }}>
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            {[
              { num: '01', title: 'Ingest & compile', desc: 'Connect your data sources or use our pre-built exchange integrations. The Compiler normalizes everything into a consistent schema.', tags: ['Binance', 'Coinbase', 'OKX', 'Glassnode', 'CSV'] },
              { num: '02', title: 'Define & simulate', desc: 'Configure strategy parameters, universe, and constraints. Run full walk-forward simulations with statistical significance testing.', tags: ['Param sweep', 'Regime filters', 'Cost modeling'] },
              { num: '03', title: 'Audit & validate', desc: 'Comprehensive risk audit report. DSR validation, overfitting detection, and tail risk analysis with institutional-grade rigor.', tags: ['DSR test', 'Overfitting score', 'Audit report'] },
              { num: '04', title: 'Allocate & monitor', desc: 'Deploy validated strategies through the Allocator and track live performance against backtested benchmarks in Manager.', tags: ['Portfolio optimizer', 'Live dashboard', 'Risk alerts'] },
            ].map((step, i, arr) => (
              <div key={step.num} className="lp-step" style={{
                display: 'grid', gridTemplateColumns: '44px 1fr', gap: 0,
                padding: '2rem 0',
                borderTop: i === 0 ? '1px solid var(--line)' : 'none',
                borderBottom: '1px solid var(--line)',
                cursor: 'default',
              }}>
                <span className="lp-step-num" style={{ fontSize: 10, color: 'var(--t3)', fontWeight: 700, letterSpacing: '0.1em', paddingTop: 2, transition: 'color 0.3s' }}>
                  {step.num}
                </span>
                <div>
                  <h3 style={{ fontSize: 13, fontWeight: 700, color: 'var(--t1)', marginBottom: '0.5rem', transition: 'color 0.3s', letterSpacing: 0 }}>
                    {step.title}
                  </h3>
                  <p style={{ fontSize: 11, color: 'var(--t3)', lineHeight: 1.75, transition: 'color 0.3s' }}>
                    {step.desc}
                  </p>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, marginTop: '0.75rem' }}>
                    {step.tags.map((tag) => (
                      <span key={tag} className="lp-tag" style={{
                        fontSize: 9, letterSpacing: '0.07em', padding: '3px 8px',
                        border: '1px solid var(--line2)', color: 'var(--t3)',
                        textTransform: 'uppercase', transition: 'color 0.3s',
                      }}>{tag}</span>
                    ))}
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Audit panel */}
          <div style={{ background: 'var(--bg1)', border: '1px solid var(--line2)', overflow: 'hidden', position: 'sticky', top: 100 }}>
            <div style={{ padding: '18px 24px', borderBottom: '1px solid var(--line)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: 'var(--t2)' }}>
                Audit output — momentum_v3
              </span>
              <span style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 9, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: 'var(--green)' }}>
                <span className="lp-breathe-fast" style={{ width: 5, height: 5, borderRadius: '50%', background: 'var(--green)', flexShrink: 0 }} />
                Approved
              </span>
            </div>
            <div>
              {[
                { key: 'Sharpe ratio', val: '3.52', cls: 'g', badge: 'DSR ✓' },
                { key: 'Sortino ratio', val: '10.41', cls: 'g' },
                { key: 'Max drawdown', val: '−29.2%', cls: 'r' },
                { key: 'Win rate', val: '85.7%', cls: 'g' },
                { key: 'Calmar ratio', val: '69.50', cls: 'n' },
                { key: 'Walk-forward (8 folds)', val: '3.39 mean — 100% positive', cls: 'g' },
                { key: 'Param sensitivity', val: 'Low — stable', cls: 'g' },
                { key: 'Regime stability', val: 'All environments', cls: 'g' },
                { key: 'DSR (overfit prob)', val: '99.6% — clean', cls: 'g' },
              ].map((row, i, arr) => (
                <div key={row.key} className="lp-ap-row" style={{
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  padding: '13px 24px',
                  borderBottom: i < arr.length - 1 ? '1px solid var(--line)' : 'none',
                  gap: '2rem', transition: 'background 0.2s',
                }}>
                  <span style={{ fontSize: 10, color: 'var(--t2)', letterSpacing: '0.04em', whiteSpace: 'nowrap' }}>{row.key}</span>
                  <span style={{
                    fontSize: 13, fontWeight: 700, whiteSpace: 'nowrap', letterSpacing: -0.2,
                    color: row.cls === 'g' ? 'var(--green)' : row.cls === 'r' ? 'var(--t1)' : 'var(--t1)',
                  }}>
                    {row.val}
                    {row.badge && (
                      <span style={{
                        fontSize: 8, fontWeight: 700, letterSpacing: '0.1em',
                        background: 'var(--green-dim)', color: 'var(--green)',
                        padding: '2px 7px', marginLeft: 6, border: '1px solid rgba(0,200,150,0.2)',
                      }}>{row.badge}</span>
                    )}
                  </span>
                </div>
              ))}
            </div>
            <div style={{
              padding: '20px 24px', background: 'var(--green-dim)',
              borderTop: '1px solid rgba(0,200,150,0.15)',
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            }}>
              <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.14em', textTransform: 'uppercase', color: 'var(--green)' }}>Verdict</span>
              <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--green)', letterSpacing: '0.08em' }}>Approved for allocation</span>
            </div>
          </div>
        </div>
      </div>

      <div style={{ width: '100%', height: 1, background: 'var(--line)', position: 'relative', zIndex: 1 }} />

      {/* Metrics */}
      <div className="lp-section" id="metrics" style={{
        position: 'relative', zIndex: 1, padding: '120px 4rem',
        maxWidth: 1200, margin: '0 auto', borderTop: '1px solid var(--line)',
      }}>
        <div style={{ fontSize: 9, letterSpacing: '0.18em', textTransform: 'uppercase', color: 'var(--t3)', fontWeight: 700, marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ width: 20, height: 1, background: 'var(--t3)', flexShrink: 0 }} />
          By the numbers
        </div>
        <h2 style={{ fontSize: 'clamp(28px, 3.5vw, 48px)', fontWeight: 700, lineHeight: 1.08, letterSpacing: -0.8, color: 'var(--t0)', marginBottom: '1.25rem' }}>
          Built for institutional<br />capital at scale.
        </h2>

        <div className="lp-metrics-split" style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '8rem', alignItems: 'start', marginTop: '5rem' }}>
          <div>
            {[
              { num: '5', suffix: 'B+', label: 'Data points / day' },
              { num: '70', suffix: '+', label: 'Risk metrics / audit' },
              { num: '2', suffix: '', label: 'Supported exchanges' },
              { num: '99', suffix: '.9%', label: 'Uptime SLA' },
            ].map((m, i) => (
              <div key={m.label} style={{
                padding: '2.5rem 0',
                borderTop: i === 0 ? '1px solid var(--line)' : 'none',
                borderBottom: '1px solid var(--line)',
              }}>
                <div style={{ fontSize: 48, fontWeight: 700, color: 'var(--t0)', letterSpacing: -2, lineHeight: 1, marginBottom: 6 }}>
                  {m.num}<span style={{ fontSize: 28, color: 'var(--green)', letterSpacing: -1 }}>{m.suffix}</span>
                </div>
                <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: 'var(--t3)' }}>{m.label}</div>
              </div>
            ))}
          </div>

          <div style={{ borderTop: '1px solid var(--line)', borderBottom: '1px solid var(--line)' }}>
            {[
              { quote: "3M's walk-forward validation caught overfitting in a strategy our team was ready to deploy. The DSR check alone saved us from a significant drawdown.", initials: 'AR', name: 'Alex R.', role: 'Quant PM — Crypto Hedge Fund', color: 'rgba(0,194,255,0.1)', textColor: '#00C2FF' },
              { quote: "The regime robustness module is exceptional. We now require every strategy to pass 3M's audit before it sees live capital.", initials: 'JK', name: 'James K.', role: 'CIO — Digital Asset Fund', color: 'var(--green-dim)', textColor: 'var(--green)' },
              { quote: "Finally a platform that speaks the language of institutional risk management. The Allocator replaced three separate tools for our family office.", initials: 'ML', name: 'Maya L.', role: 'Head of Risk — Family Office', color: 'rgba(167,139,255,0.1)', textColor: '#A78BFF' },
            ].map((t, i, arr) => (
              <div key={t.name} style={{ padding: '2.5rem 0', borderBottom: i < arr.length - 1 ? '1px solid var(--line)' : 'none' }}>
                <p style={{ fontSize: 13, color: 'var(--t1)', lineHeight: 1.85, marginBottom: '1.25rem', fontStyle: 'italic' }}>
                  &ldquo;{t.quote}&rdquo;
                </p>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <div style={{
                    width: 28, height: 28, borderRadius: '50%',
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: 9, fontWeight: 700, flexShrink: 0,
                    background: t.color, color: t.textColor,
                  }}>{t.initials}</div>
                  <div>
                    <div style={{ fontSize: 11, color: 'var(--t0)', fontWeight: 700 }}>{t.name}</div>
                    <div style={{ fontSize: 10, color: 'var(--t2)' }}>{t.role}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div style={{ width: '100%', height: 1, background: 'var(--line)', position: 'relative', zIndex: 1 }} />

      {/* Pricing */}
      <div className="lp-section" id="pricing" style={{
        position: 'relative', zIndex: 1, padding: '120px 4rem',
        maxWidth: 1200, margin: '0 auto', borderTop: '1px solid var(--line)',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', gap: '3rem', marginBottom: '5rem', flexWrap: 'wrap' }}>
          <div>
            <div style={{ fontSize: 9, letterSpacing: '0.18em', textTransform: 'uppercase', color: 'var(--t3)', fontWeight: 700, marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: 12 }}>
              <span style={{ width: 20, height: 1, background: 'var(--t3)', flexShrink: 0 }} />
              Access tiers
            </div>
            <h2 style={{ fontSize: 'clamp(28px, 3.5vw, 48px)', fontWeight: 700, lineHeight: 1.08, letterSpacing: -0.8, color: 'var(--t0)', marginBottom: '1.25rem' }}>
              Your fund&rsquo;s name. Our trade engine.<br />White-label the full stack.
            </h2>
          </div>
          <p style={{ fontSize: 12, color: 'var(--t1)', lineHeight: 1.9, maxWidth: 440 }}>
            From solo researchers to multi-PM institutional platforms. No commitment required to start.
          </p>
        </div>

        <div className="lp-pricing-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 1, background: 'var(--line)' }}>
          {[
            {
              tier: 'Research', price: 'Free', cycle: 'Forever free — no card required', featured: false,
              features: [
                { on: true, text: 'Simulator (5 audits/month)' },
                { on: true, text: 'Walk-forward backtesting' },
                { on: true, text: 'Core risk metrics (15+)' },
                { on: false, text: 'CSV data upload only' },
                { on: false, text: 'Community support' },
                { on: false, text: 'Indexer module', dim: true },
                { on: false, text: 'Allocator module', dim: true },
                { on: false, text: 'API access', dim: true },
              ],
              cta: 'Get started', ctaHref: '#waitlist',
            },
            {
              tier: 'Professional', price: '$1,999', priceSub: '/mo', cycle: 'Billed monthly — cancel anytime', featured: true,
              features: [
                { on: true, text: 'Unlimited strategy audits' },
                { on: true, text: 'All 5 platform modules' },
                { on: true, text: '40+ risk metrics' },
                { on: true, text: 'Live exchange data feeds' },
                { on: true, text: 'Allocator & portfolio optimizer' },
                { on: true, text: 'API access (10k calls/mo)' },
                { on: true, text: 'Priority support' },
                { on: false, text: 'White-label reporting' },
              ],
              cta: 'Start free trial', ctaHref: '#waitlist',
            },
            {
              tier: 'Institutional', price: 'Custom', cycle: 'Annual contracts — volume pricing', featured: false,
              features: [
                { on: true, text: 'Everything in Professional' },
                { on: true, text: 'Dedicated infrastructure' },
                { on: true, text: 'Unlimited API access' },
                { on: true, text: 'Custom data integrations' },
                { on: true, text: 'White-label deployment' },
                { on: true, text: 'SLA guarantees (99.9%)' },
                { on: true, text: 'Onboarding & training' },
                { on: true, text: 'Dedicated account manager' },
              ],
              cta: 'Contact sales', ctaHref: 'mailto:sales@mullincap.com',
            },
          ].map((card) => (
            <div key={card.tier} className="lp-pricing-card" style={{
              background: card.featured ? 'var(--bg1)' : 'var(--bg0)',
              padding: '2.5rem 2rem', display: 'flex', flexDirection: 'column',
              outline: card.featured ? '1px solid var(--line2)' : 'none',
              outlineOffset: card.featured ? -1 : 0,
            }}>
              {card.featured && (
                <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: 'var(--green)', marginBottom: '1.5rem', display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ width: 4, height: 4, borderRadius: '50%', background: 'var(--green)' }} />
                  Most popular
                </div>
              )}
              <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: '0.14em', textTransform: 'uppercase', color: 'var(--t3)', marginBottom: '1rem' }}>{card.tier}</div>
              <div style={{ fontSize: 36, fontWeight: 700, color: 'var(--t0)', letterSpacing: -1, lineHeight: 1, marginBottom: '0.25rem' }}>
                {card.price}{card.priceSub && <sub style={{ fontSize: 13, fontWeight: 400, color: 'var(--t2)', letterSpacing: 0, verticalAlign: 'baseline' }}>{card.priceSub}</sub>}
              </div>
              <div style={{ fontSize: 10, color: 'var(--t3)', marginBottom: '2.5rem', letterSpacing: '0.04em' }}>{card.cycle}</div>
              <div style={{ height: 1, background: 'var(--line)', marginBottom: '2rem' }} />
              <ul style={{ listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 11, flex: 1, marginBottom: '2.5rem' }}>
                {card.features.map((f) => (
                  <li key={f.text} style={{
                    fontSize: 11, display: 'flex', alignItems: 'flex-start', gap: 8, lineHeight: 1.5,
                    color: f.on ? 'var(--t1)' : 'var(--t2)',
                    opacity: (f as { dim?: boolean }).dim ? 0.35 : 1,
                  }}>
                    <span style={{ flexShrink: 0, color: f.on ? 'var(--green)' : 'var(--t3)' }}>{f.on ? '✓' : '—'}</span>
                    {f.text}
                  </li>
                ))}
              </ul>
              <a href={card.ctaHref} className={card.featured ? 'lp-p-cta-featured' : 'lp-p-cta'} style={{
                display: 'block', textAlign: 'center', padding: '12px 24px',
                fontFamily: 'var(--font-space-mono)', fontSize: 10, letterSpacing: '0.12em',
                textTransform: 'uppercase', fontWeight: 700, textDecoration: 'none',
                border: card.featured ? 'none' : '1px solid var(--line2)',
                color: card.featured ? '#000' : 'var(--t2)',
                background: card.featured ? 'var(--green)' : 'transparent',
              }}>{card.cta}</a>
            </div>
          ))}
        </div>
      </div>

      <div style={{ width: '100%', height: 1, background: 'var(--line)', position: 'relative', zIndex: 1 }} />

      {/* Waitlist */}
      <div id="waitlist" className="lp-waitlist" style={{
        position: 'relative', zIndex: 1, padding: '140px 4rem',
        maxWidth: 1200, margin: '0 auto',
        display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center',
        borderTop: '1px solid var(--line)',
      }}>
        <div style={{ fontSize: 9, letterSpacing: '0.18em', textTransform: 'uppercase', color: 'var(--t3)', fontWeight: 700, marginBottom: '2rem', display: 'flex', alignItems: 'center', gap: 12, justifyContent: 'center' }}>
          Early access
        </div>
        <h2 style={{ fontSize: 'clamp(28px, 4vw, 54px)', fontWeight: 700, lineHeight: 1.05, letterSpacing: -1, color: 'var(--t0)', maxWidth: 760, marginBottom: '1.25rem' }}>
          Audit your risk.<br />Compound your profits.
        </h2>
        <p style={{ fontSize: 12, color: 'var(--t1)', maxWidth: 380, lineHeight: 1.9, marginBottom: '3rem' }}>
          Join the waitlist for early access to the full 3M platform. We onboard a limited number of new funds each month.
        </p>
        <div className="lp-waitlist-form" style={{ display: 'flex', maxWidth: 400, width: '100%', gap: 0 }}>
          <input
            type="email"
            placeholder="your@fund.com"
            value={wlEmail}
            onChange={(e) => setWlEmail(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') handleWL(); }}
            disabled={wlSubmitted}
            style={{
              flex: 1, background: 'var(--bg2)', border: '1px solid var(--line2)', borderRight: 'none',
              color: 'var(--t0)', fontFamily: 'var(--font-space-mono)', fontSize: 12,
              padding: '13px 18px', outline: 'none',
            }}
          />
          <button
            onClick={handleWL}
            disabled={wlSubmitted}
            style={{
              padding: '13px 22px', background: 'var(--green)', color: '#000',
              border: 'none', fontFamily: 'var(--font-space-mono)', fontSize: 10,
              fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase',
              cursor: wlSubmitted ? 'default' : 'pointer',
              opacity: wlSubmitted ? 0.4 : 1, whiteSpace: 'nowrap',
            }}
          >
            Join Waitlist
          </button>
        </div>
        <p style={{ fontSize: 10, color: wlMsgColor, marginTop: '1rem', letterSpacing: '0.04em' }}>{wlMsg}</p>
      </div>

      {/* Footer */}
      <footer className="lp-footer" style={{
        position: 'relative', zIndex: 1, borderTop: '1px solid var(--line)',
        padding: '2rem 4rem', display: 'flex', justifyContent: 'space-between',
        alignItems: 'center', gap: '1rem', flexWrap: 'wrap',
      }}>
        <a href="#" style={{
          fontSize: 13, fontWeight: 700, color: 'var(--t0)', textDecoration: 'none',
          letterSpacing: '0.08em', textTransform: 'uppercase',
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          <span className="lp-breathe" style={{
            width: 6, height: 6, borderRadius: '50%', background: 'var(--green)', flexShrink: 0,
          }} />
          3M
        </a>
        <p style={{ fontSize: 10, color: 'var(--t3)', letterSpacing: '0.04em' }}>© {new Date().getFullYear()} Mullin Capital. All rights reserved.</p>
        <ul className="lp-footer-links" style={{ display: 'flex', gap: '2rem', listStyle: 'none' }}>
          <li>
            <Link href="/privacy" className="lp-footer-link" style={{
              fontSize: 10, color: 'var(--t3)', textDecoration: 'none',
              letterSpacing: '0.08em', textTransform: 'uppercase',
            }}>Privacy</Link>
          </li>
          <li>
            <Link href="/terms" className="lp-footer-link" style={{
              fontSize: 10, color: 'var(--t3)', textDecoration: 'none',
              letterSpacing: '0.08em', textTransform: 'uppercase',
            }}>Terms</Link>
          </li>
          <li>
            <a href="mailto:j@mullincap.com" className="lp-footer-link" style={{
              fontSize: 10, color: 'var(--t3)', textDecoration: 'none',
              letterSpacing: '0.08em', textTransform: 'uppercase',
            }}>Contact</a>
          </li>
          <li>
            <Link href="/trader/overview" className="lp-footer-link" style={{
              fontSize: 10, color: 'var(--t3)', textDecoration: 'none',
              letterSpacing: '0.08em', textTransform: 'uppercase',
            }}>App</Link>
          </li>
        </ul>
      </footer>
    </>
  );
}

'use client';

import { useEffect, useRef, useState } from 'react';

export default function Topbar() {
  const [open, setOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const items: Array<{ key: string; icon: string }> = [
    { key: 'compiler', icon: '</>' },
    { key: 'indexer', icon: '⌕' },
    { key: 'simulator', icon: '◴' },
    { key: 'traders', icon: '⚙' },
    { key: 'projector', icon: '▣' },
  ];

  useEffect(() => {
    function onDocPointerDown(e: MouseEvent) {
      if (!menuRef.current) return;
      if (!menuRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    function onEsc(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false);
    }
    document.addEventListener('mousedown', onDocPointerDown);
    document.addEventListener('keydown', onEsc);
    return () => {
      document.removeEventListener('mousedown', onDocPointerDown);
      document.removeEventListener('keydown', onEsc);
    };
  }, []);

  return (
    <div
      style={{
        height: 46,
        background: 'var(--bg1)',
        borderBottom: '1px solid var(--line)',
        padding: '0 16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexShrink: 0,
      }}
    >
      <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--t0)', letterSpacing: '0.05em' }}>
        BENJI3M
      </span>
      <div ref={menuRef} style={{ position: 'relative' }}>
        <button
          onClick={() => setOpen((v) => !v)}
          style={{
            height: 28,
            padding: '0 10px',
            border: '1px solid var(--line2)',
            borderRadius: 3,
            background: 'var(--bg1)',
            color: 'var(--t1)',
            fontSize: 9,
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
            cursor: 'pointer',
            display: 'inline-flex',
            alignItems: 'center',
            gap: 6,
          }}
        >
          <span style={{ opacity: 0.85 }}>☰</span>
          Modules {open ? '▴' : '▾'}
        </button>
        {open && (
          <div
            style={{
              position: 'absolute',
              right: 0,
              top: 34,
              minWidth: 140,
              background: 'var(--bg2)',
              border: '1px solid var(--line2)',
              borderRadius: 3,
              padding: 4,
              zIndex: 60,
              boxShadow: '0 10px 24px rgba(0,0,0,0.35)',
            }}
          >
            {items.map((item) => (
              <button
                key={item.key}
                onClick={() => setOpen(false)}
                style={{
                  width: '100%',
                  height: 28,
                  border: 'none',
                  background: 'transparent',
                  color: 'var(--t1)',
                  textAlign: 'left',
                  padding: '0 8px',
                  fontSize: 10,
                  textTransform: 'capitalize',
                  borderRadius: 2,
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                }}
                onMouseEnter={(e) => ((e.currentTarget as HTMLButtonElement).style.background = 'var(--bg3)')}
                onMouseLeave={(e) => ((e.currentTarget as HTMLButtonElement).style.background = 'transparent')}
              >
                <span
                  style={{
                    width: 14,
                    textAlign: 'center',
                    color: 'var(--t2)',
                    fontSize: 10,
                  }}
                >
                  {item.icon}
                </span>
                <span>{item.key}</span>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

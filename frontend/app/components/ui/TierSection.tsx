'use client';

import { useState } from 'react';

interface TierSectionProps {
  title: string;
  color: string;
  subtitle: string;
  children: React.ReactNode;
  defaultExpanded?: boolean;
}

export default function TierSection({ title, color, subtitle, children, defaultExpanded = false }: TierSectionProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);

  return (
    <div>
      <div
        onClick={() => setExpanded((e) => !e)}
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '8px 12px',
          cursor: 'pointer',
          background: 'var(--bg1)',
          borderTop: '1px solid var(--line)',
          borderBottom: '1px solid var(--line)',
          userSelect: 'none',
        }}
      >
        <span style={{ fontSize: 9 }}>
          <span
            style={{
              color,
              textTransform: 'uppercase',
              letterSpacing: '0.12em',
              fontWeight: 700,
            }}
          >
            {title}
          </span>
          <span style={{ color: 'var(--t3)', marginLeft: 4 }}> — {subtitle}</span>
        </span>
        <svg
          width="8"
          height="8"
          viewBox="0 0 8 8"
          fill="none"
          style={{
            transform: expanded ? 'rotate(90deg)' : 'rotate(0deg)',
            transition: 'transform 0.25s',
            flexShrink: 0,
          }}
        >
          <path d="M2 1.5L6 4L2 6.5" stroke="var(--t2)" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </div>
      <div
        style={{
          overflow: 'hidden',
          maxHeight: expanded ? '9999px' : '0',
          transition: 'max-height 0.25s ease',
        }}
      >
        {children}
      </div>
    </div>
  );
}

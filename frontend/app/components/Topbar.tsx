'use client';

export default function Topbar() {

  return (
    <div
      style={{
        height: 46,
        background: 'var(--bg1)',
        borderBottom: '1px solid var(--line)',
        padding: '0 16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'flex-start',
        flexShrink: 0,
      }}
    >
      <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--t0)', letterSpacing: '0.05em' }}>
        BENJI3M
      </span>
    </div>
  );
}

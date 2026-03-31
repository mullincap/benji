'use client';

export default function EmptyState() {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        width: '100%',
      }}
    >
      <div
        style={{
          position: 'relative',
          width: 200,
          height: 120,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          gap: 8,
        }}
      >
        {/* Corner brackets */}
        {/* Top-left */}
        <div style={{ position: 'absolute', top: 0, left: 0, width: 20, height: 20, borderTop: '1px solid var(--line)', borderLeft: '1px solid var(--line)' }} />
        {/* Top-right */}
        <div style={{ position: 'absolute', top: 0, right: 0, width: 20, height: 20, borderTop: '1px solid var(--line)', borderRight: '1px solid var(--line)' }} />
        {/* Bottom-left */}
        <div style={{ position: 'absolute', bottom: 0, left: 0, width: 20, height: 20, borderBottom: '1px solid var(--line)', borderLeft: '1px solid var(--line)' }} />
        {/* Bottom-right */}
        <div style={{ position: 'absolute', bottom: 0, right: 0, width: 20, height: 20, borderBottom: '1px solid var(--line)', borderRight: '1px solid var(--line)' }} />

        {/* Icon */}
        <div
          style={{
            width: 24,
            height: 24,
            border: '1px solid var(--t3)',
            borderRadius: 3,
          }}
        />

        {/* Text */}
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 12, color: 'var(--t2)', marginBottom: 4 }}>
            Configure parameters and run the audit
          </div>
          <div style={{ fontSize: 10, color: 'var(--t3)' }}>
            → Fill the left panel and click RUN AUDIT
          </div>
        </div>
      </div>
    </div>
  );
}

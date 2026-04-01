'use client';

interface MetricCardProps {
  label: string;
  value: string | number | null | undefined;
  unit?: string;
  unitColor?: string;
  secondary?: string;
  color?: string;
}

export default function MetricCard({
  label,
  value,
  unit,
  unitColor = 'var(--t2)',
  secondary,
  color = 'var(--green)',
}: MetricCardProps) {
  return (
    <div
      style={{
        background: 'var(--bg2)',
        border: '1px solid var(--line)',
        borderRadius: 3,
        padding: 12,
        position: 'relative',
        overflow: 'hidden',
      }}
    >
      <div
        style={{
          fontSize: 9,
          textTransform: 'uppercase',
          color: 'var(--t3)',
          letterSpacing: '0.12em',
          fontWeight: 700,
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 4 }}>
        <span
          style={{
            fontSize: 18,
            fontWeight: 700,
            color: value === null || value === undefined || value === 'N/A' ? 'var(--t2)' : color,
          }}
        >
          {value === null || value === undefined ? 'N/A' : value}
        </span>
        {unit && (
          <span style={{ fontSize: 10, color: unitColor }}>{unit}</span>
        )}
        {secondary && (
          <span style={{ fontSize: 10, color: 'var(--t3)' }}>{secondary}</span>
        )}
      </div>
    </div>
  );
}

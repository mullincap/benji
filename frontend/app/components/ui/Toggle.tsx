'use client';

interface ToggleProps {
  checked: boolean;
  onChange: (val: boolean) => void;
  disabled?: boolean;
}

export default function Toggle({ checked, onChange, disabled }: ToggleProps) {
  return (
    <div
      onClick={() => !disabled && onChange(!checked)}
      style={{
        width: 26,
        height: 14,
        borderRadius: 7,
        border: `1px solid ${checked ? 'var(--green)' : 'var(--line2)'}`,
        background: checked ? 'var(--green-mid)' : 'var(--bg4)',
        position: 'relative',
        cursor: disabled ? 'not-allowed' : 'pointer',
        flexShrink: 0,
        opacity: disabled ? 0.4 : 1,
        transition: 'background 0.2s, border-color 0.2s',
      }}
    >
      <div
        style={{
          width: 10,
          height: 10,
          borderRadius: '50%',
          background: checked ? 'var(--green)' : 'var(--t2)',
          position: 'absolute',
          top: 1,
          left: 1,
          transform: checked ? 'translateX(12px)' : 'translateX(0)',
          transition: 'transform 0.2s, background 0.2s',
        }}
      />
    </div>
  );
}

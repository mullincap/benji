'use client';

interface ConditionalParamsProps {
  show: boolean;
  children: React.ReactNode;
}

export default function ConditionalParams({ show, children }: ConditionalParamsProps) {
  return (
    <div
      style={{
        overflow: 'hidden',
        maxHeight: show ? '500px' : '0',
        transition: 'max-height 0.3s ease',
        paddingLeft: show ? 12 : 0,
        borderLeft: show ? '1px solid var(--line2)' : '1px solid transparent',
      }}
    >
      {children}
    </div>
  );
}

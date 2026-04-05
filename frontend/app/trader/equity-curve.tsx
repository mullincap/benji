"use client";

export default function EquityCurveSvg({ data }: { data: number[] }) {
  const W = 800, H = 120;
  const PAD = { top: 8, right: 8, bottom: 8, left: 8 };

  const minV = Math.min(...data) * 0.99;
  const maxV = Math.max(...data) * 1.01;
  const xs = data.map((_, i) => PAD.left + (i / (data.length - 1)) * (W - PAD.left - PAD.right));
  const ys = data.map(v => PAD.top + (1 - (v - minV) / (maxV - minV)) * (H - PAD.top - PAD.bottom));

  const linePath = xs.map((x, i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${ys[i].toFixed(1)}`).join(" ");
  const areaPath = `${linePath} L${xs[xs.length - 1].toFixed(1)},${H} L${xs[0].toFixed(1)},${H} Z`;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" height="100%" preserveAspectRatio="none" style={{ display: "block" }}>
      <defs>
        <linearGradient id="eq-fill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="var(--green)" stopOpacity="0.18" />
          <stop offset="100%" stopColor="var(--green)" stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={areaPath} fill="url(#eq-fill)" />
      <path d={linePath} fill="none" stroke="var(--green)" strokeWidth={1.5} />
    </svg>
  );
}

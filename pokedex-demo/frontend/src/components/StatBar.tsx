interface StatBarProps {
  label: string;
  value: number;
  max?: number;
}

const STAT_COLORS: Record<string, string> = {
  HP:     "#FF5959",
  ATK:    "#F5AC78",
  DEF:    "#FAE078",
  "SP.A": "#9DB7F5",
  "SP.D": "#A7DB8D",
  SPD:    "#FA92B2",
};

export default function StatBar({ label, value, max = 255 }: StatBarProps) {
  const pct = Math.min((value / max) * 100, 100);
  const color = STAT_COLORS[label] ?? "#98c820";

  return (
    <div className="flex items-center gap-3">
      <span className="w-10 text-right text-[9px] font-pixel text-gray-400 shrink-0">
        {label}
      </span>
      <span className="w-8 text-right text-[10px] font-mono text-white shrink-0">
        {value}
      </span>
      <div className="flex-1 h-3 bg-gray-800 rounded-full overflow-hidden">
        <div
          className="h-full rounded-full stat-bar-fill"
          style={
            {
              "--bar-width": `${pct}%`,
              backgroundColor: color,
              boxShadow: `0 0 6px ${color}88`,
            } as React.CSSProperties
          }
        />
      </div>
    </div>
  );
}

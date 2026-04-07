interface TypeBadgeProps {
  type: string;
  color: string;
  size?: "sm" | "md";
}

export default function TypeBadge({ type, color, size = "md" }: TypeBadgeProps) {
  const padding = size === "sm" ? "px-2 py-0.5 text-[9px]" : "px-3 py-1 text-[10px]";
  return (
    <span
      className={`type-badge inline-block font-bold uppercase tracking-wider rounded-full ${padding}`}
      style={{ backgroundColor: color, color: "#fff" }}
    >
      {type}
    </span>
  );
}

import { PokemonMove } from "../types";

interface MovesListProps {
  moves: PokemonMove[];
}

export default function MovesList({ moves }: MovesListProps) {
  return (
    <div className="space-y-1 max-h-64 overflow-y-auto pr-1">
      <div className="grid grid-cols-[1fr_80px_50px_40px_40px_35px] gap-1 text-[8px] font-pixel text-gray-500 pb-1 border-b border-gray-700 sticky top-0 bg-gray-900 z-10">
        <span>Move</span>
        <span>Type</span>
        <span className="text-center">Cat</span>
        <span className="text-center">PWR</span>
        <span className="text-center">ACC</span>
        <span className="text-center">PP</span>
      </div>
      {moves.map((move, i) => (
        <div
          key={i}
          className="grid grid-cols-[1fr_80px_50px_40px_40px_35px] gap-1 items-center text-[9px] py-0.5 hover:bg-gray-800 rounded px-0.5 transition-colors"
        >
          <span className="text-white truncate">{move.name}</span>
          <span
            className="text-[8px] px-1.5 py-0.5 rounded-full text-center font-bold"
            style={{ backgroundColor: move.type_color, color: "#fff" }}
          >
            {move.type}
          </span>
          <span className="text-center text-base" title={move.category}>
            {move.category_icon}
          </span>
          <span className="text-center text-gray-300">{move.power ?? "—"}</span>
          <span className="text-center text-gray-300">
            {move.accuracy != null ? `${move.accuracy}%` : "—"}
          </span>
          <span className="text-center text-gray-400">{move.pp ?? "—"}</span>
        </div>
      ))}
    </div>
  );
}

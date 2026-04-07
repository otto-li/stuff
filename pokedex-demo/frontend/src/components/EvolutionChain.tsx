import { EvolutionEntry } from "../types";

interface EvolutionChainProps {
  chain: EvolutionEntry[];
  currentName: string;
}

export default function EvolutionChain({ chain, currentName }: EvolutionChainProps) {
  if (chain.length <= 1) return null;

  return (
    <div className="flex items-center justify-center gap-2 flex-wrap">
      {chain.map((entry, i) => (
        <div key={entry.name} className="flex items-center gap-2">
          <div
            className={`flex flex-col items-center gap-1 p-2 rounded-lg transition-all ${
              entry.name === currentName
                ? "bg-yellow-400/20 ring-2 ring-yellow-400"
                : "bg-gray-800 hover:bg-gray-700"
            }`}
          >
            {entry.sprite ? (
              <img
                src={entry.sprite}
                alt={entry.name}
                className="w-12 h-12 object-contain"
                style={{ imageRendering: "pixelated" }}
              />
            ) : (
              <div className="w-12 h-12 flex items-center justify-center text-gray-600 text-2xl">
                ?
              </div>
            )}
            <span
              className={`text-[8px] font-pixel capitalize ${
                entry.name === currentName ? "text-yellow-400" : "text-gray-400"
              }`}
            >
              {entry.name}
            </span>
          </div>
          {i < chain.length - 1 && (
            <span className="text-gray-600 text-lg">→</span>
          )}
        </div>
      ))}
    </div>
  );
}

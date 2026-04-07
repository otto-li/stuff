import { useState } from "react";
import { PokemonData } from "../types";
import StatBar from "./StatBar";
import TypeBadge from "./TypeBadge";
import MovesList from "./MovesList";
import EvolutionChain from "./EvolutionChain";

interface PokemonCardProps {
  data: PokemonData;
}

type Tab = "stats" | "moves" | "evolution";

const STAT_LABELS: Record<string, string> = {
  hp: "HP", attack: "ATK", defense: "DEF",
  sp_atk: "SP.A", sp_def: "SP.D", speed: "SPD",
};

export default function PokemonCard({ data }: PokemonCardProps) {
  const [tab, setTab] = useState<Tab>("stats");
  const [shiny, setShiny] = useState(false);

  const primaryColor = Object.values(data.type_colors)[0] ?? "#888";
  const displaySprite = shiny ? data.sprite_shiny : (data.sprite_official ?? data.sprite);

  return (
    <div className="w-full max-w-2xl mx-auto">
      {/* Pokédex shell */}
      <div
        className="rounded-3xl p-1 shadow-2xl"
        style={{ background: "linear-gradient(135deg, #CC0000 0%, #880000 60%, #550000 100%)" }}
      >
        {/* Inner frame */}
        <div className="rounded-3xl bg-gray-950 p-4 space-y-4">

          {/* Header: number + name + legendary badge */}
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-2">
                <span className="text-gray-500 font-pixel text-[10px]">
                  #{String(data.id).padStart(3, "0")}
                </span>
                {data.generation && (
                  <span className="text-[8px] font-pixel text-gray-600 bg-gray-800 px-2 py-0.5 rounded">
                    GEN {data.generation}
                  </span>
                )}
              </div>
              <h1 className="text-2xl font-pixel text-white mt-1 capitalize">
                {data.display_name}
              </h1>
              <div className="flex gap-2 mt-2 flex-wrap">
                {data.types.map((t) => (
                  <TypeBadge key={t} type={t} color={data.type_colors[t]} />
                ))}
                {data.is_legendary && (
                  <span className="text-[9px] font-pixel px-2 py-1 rounded-full bg-yellow-400/20 text-yellow-400 border border-yellow-400/40">
                    ★ Legendary
                  </span>
                )}
              </div>
            </div>

            {/* Competitive tier */}
            <div
              className="flex flex-col items-center justify-center w-16 h-16 rounded-xl font-pixel border-2"
              style={{
                borderColor: data.tier.color,
                boxShadow: `0 0 12px ${data.tier.color}66`,
              }}
            >
              <span className="text-[8px] text-gray-400">TIER</span>
              <span
                className="text-xl font-pixel"
                style={{ color: data.tier.color }}
              >
                {data.tier.tier}
              </span>
              <span className="text-[7px] text-gray-500">{data.tier.bst} BST</span>
            </div>
          </div>

          {/* Sprite + quick info */}
          <div className="flex gap-4 items-center">
            {/* CRT screen with sprite */}
            <div
              className="relative crt rounded-xl overflow-hidden shrink-0"
              style={{
                background: "linear-gradient(135deg, #1a2e00 0%, #2a4a00 100%)",
                boxShadow: "0 0 20px #98c82066, inset 0 0 20px rgba(0,0,0,0.5)",
                width: 160,
                height: 160,
              }}
            >
              {displaySprite ? (
                <img
                  src={displaySprite}
                  alt={data.name}
                  className="w-full h-full object-contain p-2"
                  style={{ imageRendering: "pixelated" }}
                />
              ) : (
                <div className="w-full h-full flex items-center justify-center text-green-500 font-pixel text-xs">
                  NO IMAGE
                </div>
              )}
              {/* Shiny toggle */}
              {data.sprite_shiny && (
                <button
                  onClick={() => setShiny(!shiny)}
                  className={`absolute top-1 right-1 text-[8px] font-pixel px-1.5 py-0.5 rounded transition-all ${
                    shiny
                      ? "bg-yellow-400 text-black"
                      : "bg-gray-800 text-gray-400 hover:text-yellow-400"
                  }`}
                >
                  ✦
                </button>
              )}
            </div>

            {/* Quick facts */}
            <div className="flex-1 space-y-2 text-[10px]">
              <div className="grid grid-cols-2 gap-2">
                <div className="bg-gray-900 rounded-lg p-2">
                  <div className="text-gray-500 text-[8px] font-pixel">HEIGHT</div>
                  <div className="text-white font-mono">{data.height.toFixed(1)} m</div>
                </div>
                <div className="bg-gray-900 rounded-lg p-2">
                  <div className="text-gray-500 text-[8px] font-pixel">WEIGHT</div>
                  <div className="text-white font-mono">{data.weight.toFixed(1)} kg</div>
                </div>
              </div>
              {/* Abilities */}
              <div className="bg-gray-900 rounded-lg p-2">
                <div className="text-gray-500 text-[8px] font-pixel mb-1">ABILITIES</div>
                <div className="flex flex-wrap gap-1">
                  {data.abilities.map((a) => (
                    <span
                      key={a.name}
                      className={`text-[8px] px-1.5 py-0.5 rounded ${
                        a.hidden
                          ? "bg-purple-900/60 text-purple-300 border border-purple-700"
                          : "bg-gray-800 text-gray-300"
                      }`}
                    >
                      {a.name}
                      {a.hidden && " (H)"}
                    </span>
                  ))}
                </div>
              </div>
              {/* Description */}
              <div
                className="bg-gray-900 rounded-lg p-2 text-gray-300 leading-relaxed"
                style={{ fontSize: "9px" }}
              >
                {data.description}
              </div>
            </div>
          </div>

          {/* Tabs */}
          <div className="flex gap-1 border-b border-gray-800">
            {(["stats", "moves", "evolution"] as Tab[]).map((t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`px-4 py-2 text-[9px] font-pixel capitalize transition-all ${
                  tab === t
                    ? "border-b-2 text-white"
                    : "text-gray-500 hover:text-gray-300"
                }`}
                style={tab === t ? { borderColor: primaryColor, color: primaryColor } : {}}
              >
                {t}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="min-h-40">
            {tab === "stats" && (
              <div className="space-y-2">
                {Object.entries(data.stats).map(([key, val]) => (
                  <StatBar
                    key={key}
                    label={STAT_LABELS[key] ?? key}
                    value={val as number}
                  />
                ))}
                <div className="flex justify-end mt-3">
                  <div className="text-[9px] font-pixel text-gray-500">
                    TOTAL:{" "}
                    <span className="text-white">{data.bst}</span>
                  </div>
                </div>
              </div>
            )}
            {tab === "moves" && <MovesList moves={data.moves} />}
            {tab === "evolution" && (
              <div className="flex items-center justify-center py-4">
                <EvolutionChain chain={data.evolution_chain} currentName={data.name} />
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

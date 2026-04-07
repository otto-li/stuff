import { useState } from "react";
import ImageUpload from "./components/ImageUpload";
import PokemonCard from "./components/PokemonCard";
import { PokemonData } from "./types";

type AppState = "idle" | "loading" | "result" | "error";

export default function App() {
  const [state, setState] = useState<AppState>("idle");
  const [pokemon, setPokemon] = useState<PokemonData | null>(null);
  const [error, setError] = useState<string>("");
  const [searchInput, setSearchInput] = useState("");

  const handleIdentify = async (file: File) => {
    setState("loading");
    setError("");
    setPokemon(null);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const res = await fetch("/api/identify", { method: "POST", body: formData });
      if (!res.ok) {
        const detail = await res.json().then((d) => d.detail).catch(() => "Unknown error");
        throw new Error(detail);
      }

      const data: PokemonData = await res.json();
      setPokemon(data);
      setState("result");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to identify Pokémon");
      setState("error");
    }
  };

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!searchInput.trim()) return;
    setState("loading");
    setError("");
    setPokemon(null);

    try {
      const res = await fetch(`/api/pokemon/${encodeURIComponent(searchInput.trim().toLowerCase())}`);
      if (!res.ok) {
        const detail = await res.json().then((d) => d.detail).catch(() => "Pokémon not found");
        throw new Error(detail);
      }
      const data: PokemonData = await res.json();
      setPokemon(data);
      setState("result");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Pokémon not found");
      setState("error");
    }
  };

  const reset = () => {
    setState("idle");
    setPokemon(null);
    setError("");
    setSearchInput("");
  };

  return (
    <div className="min-h-screen bg-gray-950 px-4 py-8">
      {/* Header */}
      <div className="text-center mb-8">
        <div className="flex items-center justify-center gap-3 mb-2">
          {/* Mini pokéball */}
          <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-gray-600 relative shrink-0">
            <div className="w-full h-4 bg-red-600" />
            <div className="w-full h-4 bg-white" />
            <div className="absolute inset-0 flex items-center justify-center">
              <div className="w-3 h-3 rounded-full bg-gray-950 border-2 border-gray-500" />
            </div>
          </div>
          <h1 className="text-2xl font-pixel text-white tracking-wider">POKÉDEX</h1>
          <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-gray-600 relative shrink-0">
            <div className="w-full h-4 bg-red-600" />
            <div className="w-full h-4 bg-white" />
            <div className="absolute inset-0 flex items-center justify-center">
              <div className="w-3 h-3 rounded-full bg-gray-950 border-2 border-gray-500" />
            </div>
          </div>
        </div>
        <p className="text-gray-500 text-[10px] font-mono">
          Powered by{" "}
          <span className="text-red-400 font-pixel">Claude Vision</span>
          {" "}on{" "}
          <span className="text-orange-400 font-pixel">Databricks</span>
        </p>
      </div>

      <div className="max-w-2xl mx-auto space-y-6">
        {/* Manual search bar */}
        <form onSubmit={handleSearch} className="flex gap-2">
          <input
            id="pokemon-search"
            name="pokemon-search"
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Or type a Pokémon name..."
            className="flex-1 bg-gray-900 border border-gray-700 rounded-lg px-4 py-2 text-[11px] font-mono text-white placeholder-gray-600 focus:outline-none focus:border-red-500 transition-colors"
          />
          <button
            type="submit"
            disabled={state === "loading"}
            className="px-4 py-2 bg-red-700 hover:bg-red-600 disabled:opacity-50 text-white text-[9px] font-pixel rounded-lg transition-colors"
          >
            SEARCH
          </button>
        </form>

        {/* Upload or result */}
        {(state === "idle" || state === "loading" || state === "error") && (
          <ImageUpload onIdentify={handleIdentify} loading={state === "loading"} />
        )}

        {state === "error" && (
          <div className="bg-red-950/50 border border-red-800 rounded-xl p-4 text-center">
            <p className="text-red-400 font-pixel text-[10px] mb-1">ERROR</p>
            <p className="text-gray-300 text-[11px] font-mono">{error}</p>
            <button
              onClick={reset}
              className="mt-3 text-[9px] font-pixel text-gray-400 hover:text-white underline"
            >
              Try again
            </button>
          </div>
        )}

        {state === "result" && pokemon && (
          <>
            <PokemonCard data={pokemon} />
            <button
              onClick={reset}
              className="w-full py-3 text-[9px] font-pixel text-gray-500 hover:text-white border border-gray-800 hover:border-gray-600 rounded-xl transition-all"
            >
              ← SCAN ANOTHER POKÉMON
            </button>
          </>
        )}

        {/* Footer */}
        <p className="text-center text-[8px] text-gray-700 font-mono pb-4">
          Data from PokéAPI · Vision by Claude · Hosted on Databricks Apps
        </p>
      </div>
    </div>
  );
}

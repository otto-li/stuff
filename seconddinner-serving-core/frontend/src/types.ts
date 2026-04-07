export interface Card {
  card_name: string;
  cost: number;
  power: number;
  ability_text: string;
  tags: string;
  archetype: string;
}

export type ServingType =
  | "batch"
  | "model_serving"
  | "feature_serving"
  | "vector_search"
  | "foundation_model"
  | "retl";

export interface ServingConfig {
  type: ServingType;
  label: string;
  sublabel: string;
  color: string;
  textClass: string;
  glowClass: string;
  neonTextClass: string;
  borderClass: string;
}

export const SERVING_CONFIGS: Record<ServingType, ServingConfig> = {
  batch: {
    type: "batch",
    label: "BATCH INFERENCE",
    sublabel: "OFFLINE / DELTA TABLE",
    color: "#00d4ff",
    textClass: "text-cyan",
    glowClass: "shadow-neon-cyan",
    neonTextClass: "neon-text-cyan",
    borderClass: "border-cyan",
  },
  model_serving: {
    type: "model_serving",
    label: "MODEL SERVING",
    sublabel: "REAL-TIME / K-MEANS ENDPOINT",
    color: "#ff4444",
    textClass: "text-neon-red",
    glowClass: "shadow-neon-red",
    neonTextClass: "neon-text-red",
    borderClass: "border-neon-red",
  },
  feature_serving: {
    type: "feature_serving",
    label: "FEATURE SERVING",
    sublabel: "ONLINE TABLE / LAKEBASE",
    color: "#00ff88",
    textClass: "text-neon-green",
    glowClass: "shadow-neon-green",
    neonTextClass: "neon-text-green",
    borderClass: "border-neon-green",
  },
  vector_search: {
    type: "vector_search",
    label: "VECTOR SEARCH",
    sublabel: "EMBEDDINGS / SIMILARITY",
    color: "#a855f7",
    textClass: "text-neon-purple",
    glowClass: "shadow-neon-purple",
    neonTextClass: "neon-text-purple",
    borderClass: "border-neon-purple",
  },
  foundation_model: {
    type: "foundation_model",
    label: "FOUNDATION MODEL",
    sublabel: "AI GATEWAY / LLM",
    color: "#ffd700",
    textClass: "text-gold",
    glowClass: "shadow-neon-gold",
    neonTextClass: "neon-text-gold",
    borderClass: "border-gold",
  },
  retl: {
    type: "retl",
    label: "rETL / LAKEBASE",
    sublabel: "BATCH → ONLINE / POSTGRES",
    color: "#ff8800",
    textClass: "text-neon-orange",
    glowClass: "shadow-neon-orange",
    neonTextClass: "neon-text-orange",
    borderClass: "border-neon-orange",
  },
};

// Sample Marvel Snap cards for the picker
export const SAMPLE_CARDS = [
  "Deadpool",
  "Wolverine",
  "Iron Man",
  "Mystique",
  "Hela",
  "Apocalypse",
  "Carnage",
  "Venom",
  "Bucky Barnes",
  "Nova",
  "Killmonger",
  "Shang-Chi",
  "Devil Dinosaur",
  "Moon Girl",
  "Mister Negative",
  "Wong",
  "Odin",
  "Knull",
  "Destroyer",
  "Galactus",
  "Doctor Doom",
  "Magneto",
  "Sera",
  "Silver Surfer",
  "Patriot",
  "Blue Marvel",
  "Spectrum",
  "Ka-Zar",
  "Angela",
  "Lockjaw",
];

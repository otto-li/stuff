export interface PokemonStats {
  hp: number;
  attack: number;
  defense: number;
  sp_atk: number;
  sp_def: number;
  speed: number;
}

export interface PokemonMove {
  name: string;
  type: string;
  category: string;
  category_icon: string;
  power: number | null;
  accuracy: number | null;
  pp: number | null;
  type_color: string;
}

export interface EvolutionEntry {
  name: string;
  sprite: string | null;
}

export interface TierInfo {
  tier: string;
  color: string;
  bst: number;
}

export interface Ability {
  name: string;
  hidden: boolean;
}

export interface PokemonData {
  id: number;
  name: string;
  display_name: string;
  types: string[];
  type_colors: Record<string, string>;
  sprite: string | null;
  sprite_shiny: string | null;
  sprite_official: string | null;
  height: number;
  weight: number;
  description: string;
  stats: PokemonStats;
  bst: number;
  abilities: Ability[];
  moves: PokemonMove[];
  is_legendary: boolean;
  tier: TierInfo;
  evolution_chain: EvolutionEntry[];
  generation: string;
}

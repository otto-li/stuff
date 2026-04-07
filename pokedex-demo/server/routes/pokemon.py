import aiohttp
from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel
from typing import Optional
from ..llm import identify_pokemon_from_image

router = APIRouter()

POKEAPI = "https://pokeapi.co/api/v2"

TYPE_COLORS = {
    "normal": "#A8A878", "fire": "#F08030", "water": "#6890F0",
    "electric": "#F8D030", "grass": "#78C850", "ice": "#98D8D8",
    "fighting": "#C03028", "poison": "#A040A0", "ground": "#E0C068",
    "flying": "#A890F0", "psychic": "#F85888", "bug": "#A8B820",
    "rock": "#B8A038", "ghost": "#705898", "dragon": "#7038F8",
    "dark": "#705848", "steel": "#B8B8D0", "fairy": "#EE99AC",
}

MOVE_CATEGORY_ICONS = {
    "physical": "⚔️",
    "special": "✨",
    "status": "🔄",
}


def competitive_tier(bst: int, is_legendary: bool) -> dict:
    if is_legendary and bst >= 600:
        tier, color = "Uber", "#9B59B6"
    elif bst >= 540:
        tier, color = "OU", "#E74C3C"
    elif bst >= 480:
        tier, color = "UU", "#E67E22"
    elif bst >= 420:
        tier, color = "RU", "#F1C40F"
    elif bst >= 360:
        tier, color = "NU", "#2ECC71"
    else:
        tier, color = "PU", "#95A5A6"
    return {"tier": tier, "color": color, "bst": bst}


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url) as r:
        if r.status != 200:
            raise HTTPException(status_code=404, detail=f"Not found: {url}")
        return await r.json()


async def get_evolution_chain(session: aiohttp.ClientSession, chain_url: str) -> list:
    data = await fetch_json(session, chain_url)
    chain = []

    def parse_chain(link):
        name = link["species"]["name"]
        chain.append(name)
        for evo in link.get("evolves_to", []):
            parse_chain(evo)

    parse_chain(data["chain"])
    return chain


async def get_sprite(session: aiohttp.ClientSession, name: str) -> Optional[str]:
    try:
        data = await fetch_json(session, f"{POKEAPI}/pokemon/{name}")
        return data["sprites"]["front_default"]
    except Exception:
        return None


@router.post("/identify")
async def identify_pokemon(file: UploadFile = File(...)):
    """Receive an image, identify the Pokémon via Claude Vision, return full data."""
    content_type = file.content_type or "image/jpeg"
    image_bytes = await file.read()

    pokemon_name = await identify_pokemon_from_image(image_bytes, content_type)

    if pokemon_name == "unknown":
        raise HTTPException(status_code=422, detail="Could not identify a Pokémon in the image.")

    return await fetch_pokemon_data(pokemon_name)


@router.get("/pokemon/{name}")
async def get_pokemon(name: str):
    """Fetch full Pokémon data by name (for direct lookups / manual search)."""
    return await fetch_pokemon_data(name.lower().strip())


async def fetch_pokemon_data(name: str) -> dict:
    async with aiohttp.ClientSession() as session:
        # Core Pokémon data
        pokemon = await fetch_json(session, f"{POKEAPI}/pokemon/{name}")

        # Species data (descriptions, legendary flag, evolution chain URL)
        species = await fetch_json(session, pokemon["species"]["url"])

        # Pick English flavor text
        flavor_texts = [
            e["flavor_text"].replace("\n", " ").replace("\f", " ")
            for e in species.get("flavor_text_entries", [])
            if e["language"]["name"] == "en"
        ]
        description = flavor_texts[0] if flavor_texts else "No description available."

        # Types
        types = [t["type"]["name"] for t in pokemon["types"]]

        # Base stats
        stats_raw = {s["stat"]["name"]: s["base_stat"] for s in pokemon["stats"]}
        stats = {
            "hp": stats_raw.get("hp", 0),
            "attack": stats_raw.get("attack", 0),
            "defense": stats_raw.get("defense", 0),
            "sp_atk": stats_raw.get("special-attack", 0),
            "sp_def": stats_raw.get("special-defense", 0),
            "speed": stats_raw.get("speed", 0),
        }
        bst = sum(stats.values())

        # Top 20 moves (level-up moves first, then others)
        level_moves = [
            m for m in pokemon["moves"]
            if any(v["move_learn_method"]["name"] == "level-up" for v in m["version_group_details"])
        ]
        other_moves = [
            m for m in pokemon["moves"]
            if m not in level_moves
        ]
        selected_moves = (level_moves + other_moves)[:20]

        moves = []
        for m in selected_moves:
            move_data = await fetch_json(session, m["move"]["url"])
            moves.append({
                "name": move_data["name"].replace("-", " ").title(),
                "type": move_data["type"]["name"],
                "category": move_data["damage_class"]["name"] if move_data.get("damage_class") else "status",
                "category_icon": MOVE_CATEGORY_ICONS.get(
                    move_data["damage_class"]["name"] if move_data.get("damage_class") else "status", "🔄"
                ),
                "power": move_data.get("power"),
                "accuracy": move_data.get("accuracy"),
                "pp": move_data.get("pp"),
                "type_color": TYPE_COLORS.get(move_data["type"]["name"], "#888"),
            })

        # Evolution chain
        evo_chain_url = species.get("evolution_chain", {}).get("url")
        evo_chain_names = []
        evo_sprites = []
        if evo_chain_url:
            evo_chain_names = await get_evolution_chain(session, evo_chain_url)
            for evo_name in evo_chain_names:
                sprite = await get_sprite(session, evo_name)
                evo_sprites.append({"name": evo_name, "sprite": sprite})

        is_legendary = species.get("is_legendary", False) or species.get("is_mythical", False)
        tier_info = competitive_tier(bst, is_legendary)

        # Abilities
        abilities = [
            {"name": a["ability"]["name"].replace("-", " ").title(), "hidden": a["is_hidden"]}
            for a in pokemon["abilities"]
        ]

        return {
            "id": pokemon["id"],
            "name": pokemon["name"],
            "display_name": pokemon["name"].replace("-", " ").title(),
            "types": types,
            "type_colors": {t: TYPE_COLORS.get(t, "#888") for t in types},
            "sprite": pokemon["sprites"]["front_default"],
            "sprite_shiny": pokemon["sprites"]["front_shiny"],
            "sprite_official": (
                pokemon["sprites"]
                .get("other", {})
                .get("official-artwork", {})
                .get("front_default")
            ),
            "height": pokemon["height"] / 10,   # dm → m
            "weight": pokemon["weight"] / 10,   # hg → kg
            "description": description,
            "stats": stats,
            "bst": bst,
            "abilities": abilities,
            "moves": moves,
            "is_legendary": is_legendary,
            "tier": tier_info,
            "evolution_chain": evo_sprites,
            "generation": species.get("generation", {}).get("name", "").replace("generation-", "").upper(),
        }

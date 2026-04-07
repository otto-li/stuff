"""
Pokemon TCG Live — Full Dataset Generator
==========================================
Plan:
  - player_accounts:    50,000 rows  -> otto_demo.pokemon_tcg.player_accounts
  - player_activities: 500,000 rows  -> otto_demo.pokemon_tcg.player_activities
  - next_best_actions: 100,000 rows  -> otto_demo.pokemon_tcg.next_best_actions

Engine: NumPy + pandas -> Databricks Connect bridge (serverless)
Profile: fe-vm-otto-demo
Seed: 42
"""

import json
import os
import numpy as np
import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────────
CATALOG = "otto_demo"
SCHEMA = "pokemon_tcg"
SEED = 42
rng = np.random.default_rng(SEED)

NUM_PLAYERS = 50_000
NUM_ACTIVITIES = 500_000
NUM_RECOMMENDATIONS = 100_000

# ── Constants ───────────────────────────────────────────────────────────────
REGIONS = ["NA", "EU", "APAC", "LATAM"]
REGION_WEIGHTS = np.array([40, 30, 20, 10], dtype=np.float64)

RANK_TIERS = ["Beginner", "Great", "Ultra", "Master"]
RANK_WEIGHTS = np.array([30, 35, 25, 10], dtype=np.float64)

ACCOUNT_STATUSES = ["active", "suspended", "banned"]
STATUS_WEIGHTS = np.array([95, 3, 2], dtype=np.float64)

ACTIVITY_TYPES = [
    "match_played", "pack_opened", "deck_created", "deck_edited",
    "trade_completed", "daily_login", "tournament_entered",
    "card_crafted", "friend_added", "shop_purchase",
]
ACTIVITY_WEIGHTS = np.array([30, 15, 5, 8, 5, 20, 4, 5, 3, 5], dtype=np.float64)

MATCH_RESULTS = ["win", "loss", "draw"]

DECK_ARCHETYPES = [
    "Charizard ex", "Lugia VSTAR", "Gardevoir ex", "Mew VMAX",
    "Lost Zone Box", "Arceus VSTAR", "Giratina VSTAR", "Rapid Strike",
    "Miraidon ex", "Roaring Moon ex", "Iron Hands ex", "Snorlax Stall",
    "Chien-Pao ex", "Pidgeot Control", "Dragapult ex", "Regidrago VSTAR",
]

ACTION_TYPES = [
    "buy_pack", "upgrade_deck", "enter_tournament", "complete_daily",
    "trade_card", "add_friend", "try_new_deck_archetype", "claim_reward",
]
ACTION_WEIGHTS = np.array([20, 15, 10, 20, 10, 8, 10, 7], dtype=np.float64)

PACK_NAMES = [
    "Obsidian Flames", "Paldea Evolved", "Scarlet & Violet",
    "Crown Zenith", "Silver Tempest", "Lost Origin",
    "Temporal Forces", "Twilight Masquerade", "Shrouded Fable",
    "Surging Sparks", "Prismatic Evolutions",
]

REASON_TEMPLATES = {
    "buy_pack": [
        "Your collection is missing key {archetype} cards - open {pack} packs to fill gaps.",
        "New expansion {pack} just dropped! Great cards for your {archetype} deck.",
        "You haven't opened packs in {days} days. {pack} has cards you need.",
    ],
    "upgrade_deck": [
        "Your {archetype} deck win rate is {wr}% - swap in newer staples to improve.",
        "Meta shift detected: {archetype} needs updated trainer cards.",
        "Your {archetype} deck hasn't been edited in {days} days. Time for a refresh!",
    ],
    "enter_tournament": [
        "Your ELO ({elo}) qualifies you for the weekend tournament. Prize: rare promo!",
        "Tournament entry closes soon - your {archetype} deck is tournament-ready.",
        "You've won {wins} of your last 10 matches. Enter a tournament to earn rewards!",
    ],
    "complete_daily": [
        "You have {n} unclaimed daily challenges. Complete them for bonus coins.",
        "Daily login streak at risk - log in today to keep your {n}-day streak!",
        "Complete today's challenge to earn a free {pack} pack.",
    ],
    "trade_card": [
        "You have duplicate {archetype} cards - trade them for cards you need.",
        "A popular trade is available: your extra V cards for {archetype} staples.",
        "Market demand is high for cards in your collection. Good time to trade!",
    ],
    "add_friend": [
        "You have {n} friends - adding more unlocks friend battle rewards.",
        "Players in your ELO range ({elo}) are looking for practice partners.",
        "Friend battles give bonus XP. Add friends to level up faster!",
    ],
    "try_new_deck_archetype": [
        "You've only played {archetype}. Try {new_arch} - it counters the current meta.",
        "New deck archetype {new_arch} is trending with a {wr}% win rate.",
        "Diversify! Playing multiple archetypes improves your overall skill.",
    ],
    "claim_reward": [
        "You have {n} unclaimed rewards in your mailbox. Don't let them expire!",
        "Season rewards are available - claim {n} packs and {coins} coins.",
        "Battle pass tier completed! Claim your {pack} pack reward.",
    ],
}


# ════════════════════════════════════════════════════════════════════════════
# TABLE 1: player_accounts (50K rows)
# ════════════════════════════════════════════════════════════════════════════
print(f"Generating player_accounts ({NUM_PLAYERS:,} rows)...")

# Rank tiers drive correlated stats
rank_indices = rng.choice(len(RANK_TIERS), size=NUM_PLAYERS, p=RANK_WEIGHTS / RANK_WEIGHTS.sum())
rank_tiers = np.array(RANK_TIERS)[rank_indices]

# ELO correlated with rank
elo_base = np.array([800, 1000, 1300, 1600])[rank_indices]
elo_range = np.array([400, 500, 500, 600])[rank_indices]
elo_ratings = (elo_base + rng.uniform(0, 1, size=NUM_PLAYERS) * elo_range).astype(int)

# Win/loss correlated with rank
win_rate_base = np.array([0.35, 0.45, 0.55, 0.65])[rank_indices]
win_rate_noise = rng.normal(0, 0.08, size=NUM_PLAYERS)
win_rates = np.clip(win_rate_base + win_rate_noise, 0.10, 0.90)

# Total games played: higher rank = more games on average
games_base = np.array([50, 200, 500, 1200])[rank_indices]
games_noise = rng.exponential(scale=1.0, size=NUM_PLAYERS)
total_games = (games_base * (0.5 + games_noise)).astype(int)
total_games = np.clip(total_games, 5, 10000)

total_wins = (total_games * win_rates).astype(int)
total_losses = total_games - total_wins
total_draws = (total_games * rng.uniform(0.01, 0.05, size=NUM_PLAYERS)).astype(int)
total_losses = np.maximum(total_losses - total_draws, 0)
actual_win_rate = np.round(total_wins / np.maximum(total_games, 1), 4)

# Premium: higher ranks more likely
premium_prob = np.array([0.05, 0.15, 0.30, 0.55])[rank_indices]
is_premium = rng.random(size=NUM_PLAYERS) < premium_prob

# Resources
gems_base = np.where(is_premium, 2000, 500)
gems = (gems_base + rng.exponential(scale=500, size=NUM_PLAYERS)).astype(int)
coins_base = np.where(is_premium, 5000, 1000)
coins = (coins_base + rng.exponential(scale=2000, size=NUM_PLAYERS)).astype(int)

# Packs opened
packs_base = np.array([20, 80, 200, 500])[rank_indices]
packs_premium_mult = np.where(is_premium, 2.0, 1.0)
packs_opened = (packs_base * packs_premium_mult * (0.5 + rng.exponential(1.0, size=NUM_PLAYERS))).astype(int)

decks_created = np.clip(np.array([1, 3, 5, 8])[rank_indices] + rng.poisson(2, size=NUM_PLAYERS), 1, 30)
friends_count = np.clip(rng.poisson(lam=np.array([3, 8, 15, 25])[rank_indices]), 0, 100)

# Dates
account_start = np.datetime64("2023-01-01")
account_span = (np.datetime64("2025-12-31") - account_start).astype(int)
account_created = account_start + rng.integers(0, account_span + 1, size=NUM_PLAYERS).astype("timedelta64[D]")

status_indices = rng.choice(len(ACCOUNT_STATUSES), size=NUM_PLAYERS, p=STATUS_WEIGHTS / STATUS_WEIGHTS.sum())
account_status = np.array(ACCOUNT_STATUSES)[status_indices]

login_recency_days = np.where(
    account_status == "active",
    rng.integers(0, 30, size=NUM_PLAYERS),
    np.where(account_status == "suspended",
             rng.integers(30, 180, size=NUM_PLAYERS),
             rng.integers(90, 365, size=NUM_PLAYERS))
)
last_login = np.datetime64("2026-04-01") - login_recency_days.astype("timedelta64[D]")
last_login_hours = rng.integers(0, 24, size=NUM_PLAYERS).astype("timedelta64[h]")
last_login_mins = rng.integers(0, 60, size=NUM_PLAYERS).astype("timedelta64[m]")
last_login_ts = last_login + last_login_hours + last_login_mins

# Usernames
_adj = np.array(["Shadow", "Thunder", "Crystal", "Blazing", "Frost", "Storm",
                 "Mystic", "Iron", "Golden", "Dark", "Neon", "Cosmic",
                 "Rapid", "Silent", "Rogue", "Noble", "Primal", "Lunar"])
_noun = np.array(["Trainer", "Champion", "Master", "Duelist", "Collector",
                  "Ace", "Rival", "Legend", "Scout", "Ranger", "Striker",
                  "Guardian", "Seeker", "Hunter", "Tactician", "Wizard"])
adj_pick = _adj[rng.integers(0, len(_adj), size=NUM_PLAYERS)]
noun_pick = _noun[rng.integers(0, len(_noun), size=NUM_PLAYERS)]
suffix = rng.integers(1, 9999, size=NUM_PLAYERS)
usernames = np.array([f"{a}{n}{s}" for a, n, s in zip(adj_pick, noun_pick, suffix)])

_display_adj = np.array(["Cool", "Pro", "Elite", "Mega", "Ultra", "Super", "Epic", "Lucky"])
_display_noun = np.array(["Pikachu", "Charizard", "Mewtwo", "Eevee", "Gengar", "Lucario",
                          "Greninja", "Rayquaza", "Arceus", "Mew", "Dragonite", "Snorlax"])
d_adj = _display_adj[rng.integers(0, len(_display_adj), size=NUM_PLAYERS)]
d_noun = _display_noun[rng.integers(0, len(_display_noun), size=NUM_PLAYERS)]
display_names = np.array([f"{a} {n}" for a, n in zip(d_adj, d_noun)])

email_domains = np.array(["gmail.com", "yahoo.com", "outlook.com", "pokemon.fan", "tcglive.net"])
domain_pick = email_domains[rng.integers(0, len(email_domains), size=NUM_PLAYERS)]
emails = np.array([f"{u.lower()}@{d}" for u, d in zip(usernames, domain_pick)])

player_ids = np.arange(1, NUM_PLAYERS + 1)

players_df = pd.DataFrame({
    "player_id": player_ids,
    "username": usernames,
    "display_name": display_names,
    "email": emails,
    "region": rng.choice(REGIONS, size=NUM_PLAYERS, p=REGION_WEIGHTS / REGION_WEIGHTS.sum()),
    "rank_tier": rank_tiers,
    "elo_rating": elo_ratings,
    "total_wins": total_wins,
    "total_losses": total_losses,
    "total_draws": total_draws,
    "win_rate": actual_win_rate,
    "account_created_date": pd.to_datetime(account_created),
    "last_login_timestamp": pd.to_datetime(last_login_ts),
    "is_premium": is_premium,
    "gems_balance": gems,
    "coins_balance": coins,
    "packs_opened": packs_opened,
    "decks_created": decks_created,
    "friends_count": friends_count,
    "account_status": account_status,
})

print(f"  player_accounts: {len(players_df):,} rows")
print(players_df.head(5).to_string())


# ════════════════════════════════════════════════════════════════════════════
# TABLE 2: player_activities (500K rows)
# ════════════════════════════════════════════════════════════════════════════
print(f"\nGenerating player_activities ({NUM_ACTIVITIES:,} rows)...")

# Player IDs: more active players generate more events (weighted by total games)
player_activity_weights = total_games.astype(np.float64)
player_activity_weights /= player_activity_weights.sum()
activity_player_ids = rng.choice(player_ids, size=NUM_ACTIVITIES, p=player_activity_weights)

activity_types = rng.choice(ACTIVITY_TYPES, size=NUM_ACTIVITIES, p=ACTIVITY_WEIGHTS / ACTIVITY_WEIGHTS.sum())

# Timestamps: 2024-01 to 2026-03
act_start = np.datetime64("2024-01-01T00:00:00")
act_span = int((np.datetime64("2026-03-31T23:59:59") - act_start) / np.timedelta64(1, "s"))
activity_timestamps = act_start + rng.integers(0, act_span, size=NUM_ACTIVITIES).astype("timedelta64[s]")

# Match result: only for match_played
is_match = activity_types == "match_played"
match_results_arr = np.where(
    is_match,
    rng.choice(MATCH_RESULTS, size=NUM_ACTIVITIES, p=[0.48, 0.47, 0.05]),
    None,
)

# Opponent ID: only for match_played and trade_completed
needs_opponent = np.isin(activity_types, ["match_played", "trade_completed"])
opponent_ids = np.where(
    needs_opponent,
    rng.integers(1, NUM_PLAYERS + 1, size=NUM_ACTIVITIES),
    -1,
)
same_as_self = opponent_ids == activity_player_ids
opponent_ids[same_as_self & needs_opponent] = (opponent_ids[same_as_self & needs_opponent] % NUM_PLAYERS) + 1

# Deck used
needs_deck = np.isin(activity_types, ["match_played", "deck_created", "deck_edited", "tournament_entered"])
decks_used = np.where(
    needs_deck,
    rng.choice(DECK_ARCHETYPES, size=NUM_ACTIVITIES),
    None,
)

# Duration
duration = np.where(
    is_match,
    rng.integers(180, 1800, size=NUM_ACTIVITIES),
    np.where(
        activity_types == "tournament_entered",
        rng.integers(600, 3600, size=NUM_ACTIVITIES),
        rng.integers(5, 300, size=NUM_ACTIVITIES),
    ),
)

# Rewards
reward_options = ["50 coins", "100 coins", "200 coins", "1 pack", "2 packs",
                  "50 gems", "100 gems", "rare card", "promo card", "none"]
reward_weights_arr = np.array([20, 15, 8, 10, 5, 8, 4, 3, 2, 25], dtype=np.float64)
rewards = rng.choice(reward_options, size=NUM_ACTIVITIES, p=reward_weights_arr / reward_weights_arr.sum())

# Details JSON
print("  Building details_json (this takes a moment for 500K rows)...")
details_list = []
for i in range(NUM_ACTIVITIES):
    at = activity_types[i]
    detail = {}
    if at == "match_played":
        detail = {"format": str(rng.choice(["standard", "expanded", "custom"])),
                  "turns": int(rng.integers(5, 30)),
                  "first_turn": str(rng.choice(["self", "opponent"]))}
    elif at == "pack_opened":
        detail = {"pack_name": str(rng.choice(PACK_NAMES)),
                  "cards_received": int(rng.integers(5, 11)),
                  "rare_pulls": int(rng.integers(0, 4))}
    elif at == "deck_created":
        detail = {"archetype": str(rng.choice(DECK_ARCHETYPES)),
                  "card_count": 60}
    elif at == "deck_edited":
        detail = {"cards_added": int(rng.integers(1, 10)),
                  "cards_removed": int(rng.integers(1, 10))}
    elif at == "trade_completed":
        detail = {"cards_given": int(rng.integers(1, 5)),
                  "cards_received": int(rng.integers(1, 5)),
                  "trade_value_coins": int(rng.integers(50, 2000))}
    elif at == "daily_login":
        detail = {"streak_day": int(rng.integers(1, 365)),
                  "bonus_coins": int(rng.choice([10, 25, 50, 100]))}
    elif at == "tournament_entered":
        detail = {"tournament_type": str(rng.choice(["daily", "weekend", "seasonal"])),
                  "entry_fee_coins": int(rng.choice([0, 100, 250, 500]))}
    elif at == "card_crafted":
        detail = {"card_rarity": str(rng.choice(["common", "uncommon", "rare", "ultra_rare"])),
                  "dust_cost": int(rng.choice([50, 150, 500, 1500]))}
    elif at == "friend_added":
        detail = {"friend_player_id": int(rng.integers(1, NUM_PLAYERS + 1))}
    elif at == "shop_purchase":
        detail = {"item": str(rng.choice(["booster_pack", "theme_deck", "cosmetic", "battle_pass", "gems_bundle"])),
                  "cost_coins": int(rng.integers(100, 5000))}
    details_list.append(json.dumps(detail))

    if (i + 1) % 100_000 == 0:
        print(f"    ...{i+1:,} / {NUM_ACTIVITIES:,}")

activity_ids = np.arange(1, NUM_ACTIVITIES + 1)

# Convert opponent_ids: -1 -> NaN for pandas nullable int
opponent_ids_float = opponent_ids.astype(np.float64)
opponent_ids_float[opponent_ids == -1] = np.nan

activities_df = pd.DataFrame({
    "activity_id": activity_ids,
    "player_id": activity_player_ids,
    "activity_type": activity_types,
    "activity_timestamp": pd.to_datetime(activity_timestamps),
    "details_json": details_list,
    "match_result": match_results_arr,
    "opponent_id": pd.array(opponent_ids, dtype=pd.Int64Dtype()),
    "deck_used": decks_used,
    "duration_seconds": duration,
    "rewards_earned": rewards,
})
# Fix opponent_id: -1 -> null
activities_df.loc[activities_df["opponent_id"] == -1, "opponent_id"] = pd.NA

print(f"  player_activities: {len(activities_df):,} rows")
print(activities_df.head(5).to_string())


# ════════════════════════════════════════════════════════════════════════════
# TABLE 3: next_best_actions (100K rows)
# ════════════════════════════════════════════════════════════════════════════
print(f"\nGenerating next_best_actions ({NUM_RECOMMENDATIONS:,} rows)...")

rec_player_ids = rng.choice(player_ids, size=NUM_RECOMMENDATIONS)

pid_to_idx = {pid: idx for idx, pid in enumerate(player_ids)}
rec_player_indices = np.array([pid_to_idx[pid] for pid in rec_player_ids])

rec_action_types = rng.choice(ACTION_TYPES, size=NUM_RECOMMENDATIONS, p=ACTION_WEIGHTS / ACTION_WEIGHTS.sum())

rec_player_win_rates = actual_win_rate[rec_player_indices]

# Priority score with correlation
priority_scores = rng.uniform(0.3, 0.9, size=NUM_RECOMMENDATIONS)
priority_scores[(rec_action_types == "enter_tournament") & (rec_player_win_rates > 0.55)] += 0.15
priority_scores[(rec_action_types == "upgrade_deck") & (rec_player_win_rates < 0.45)] += 0.2
priority_scores[rec_action_types == "complete_daily"] += 0.1
priority_scores = np.clip(np.round(priority_scores, 3), 0.0, 1.0)

# Reason text
print("  Building reason_text...")
reason_texts = []
for i in range(NUM_RECOMMENDATIONS):
    at = rec_action_types[i]
    templates = REASON_TEMPLATES[at]
    tmpl = str(rng.choice(templates))
    txt = tmpl.format(
        archetype=str(rng.choice(DECK_ARCHETYPES)),
        new_arch=str(rng.choice(DECK_ARCHETYPES)),
        pack=str(rng.choice(PACK_NAMES)),
        days=int(rng.integers(3, 60)),
        wr=int(rec_player_win_rates[i] * 100),
        elo=int(elo_ratings[rec_player_indices[i]]),
        wins=int(rng.integers(3, 9)),
        n=int(rng.integers(1, 10)),
        coins=int(rng.choice([500, 1000, 2000, 5000])),
    )
    reason_texts.append(txt)

    if (i + 1) % 50_000 == 0:
        print(f"    ...{i+1:,} / {NUM_RECOMMENDATIONS:,}")

# Recommended card or pack
all_card_pack = PACK_NAMES + DECK_ARCHETYPES
rec_card_or_pack = np.where(
    np.isin(rec_action_types, ["buy_pack", "trade_card", "try_new_deck_archetype"]),
    rng.choice(all_card_pack, size=NUM_RECOMMENDATIONS),
    None,
)

# Estimated win rate improvement
has_wr = np.isin(rec_action_types, ["upgrade_deck", "try_new_deck_archetype", "enter_tournament"])
est_wr_improvement = np.where(
    has_wr,
    np.round(rng.uniform(0.01, 0.12, size=NUM_RECOMMENDATIONS), 3),
    np.nan,
)

# Dates
rec_start = np.datetime64("2026-03-01")
rec_span = (np.datetime64("2026-04-01") - rec_start).astype(int)
created_dates = rec_start + rng.integers(0, rec_span, size=NUM_RECOMMENDATIONS).astype("timedelta64[D]")
expires_dates = created_dates + rng.integers(3, 30, size=NUM_RECOMMENDATIONS).astype("timedelta64[D]")

is_dismissed = rng.random(size=NUM_RECOMMENDATIONS) < 0.15
is_completed = (~is_dismissed) & (rng.random(size=NUM_RECOMMENDATIONS) < 0.25)

nba_df = pd.DataFrame({
    "recommendation_id": np.arange(1, NUM_RECOMMENDATIONS + 1),
    "player_id": rec_player_ids,
    "action_type": rec_action_types,
    "priority_score": priority_scores,
    "reason_text": reason_texts,
    "recommended_card_or_pack": rec_card_or_pack,
    "estimated_win_rate_improvement": est_wr_improvement,
    "created_date": pd.to_datetime(created_dates),
    "expires_date": pd.to_datetime(expires_dates),
    "is_dismissed": is_dismissed,
    "is_completed": is_completed,
})

# NaN -> None for nullable float column
nba_df["estimated_win_rate_improvement"] = nba_df["estimated_win_rate_improvement"].where(
    nba_df["estimated_win_rate_improvement"].notna(), other=None
)

print(f"  next_best_actions: {len(nba_df):,} rows")
print(nba_df.head(5).to_string())


# ════════════════════════════════════════════════════════════════════════════
# WRITE TO UNITY CATALOG via Databricks Connect
# ════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"Writing to Unity Catalog: {CATALOG}.{SCHEMA}.*")
print(f"{'='*60}")

os.environ["DATABRICKS_CONFIG_PROFILE"] = "fe-vm-otto-demo"

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()
print("Spark session created (serverless)")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema {CATALOG}.{SCHEMA} ready")

# --- Write player_accounts ---
table1 = f"{CATALOG}.{SCHEMA}.player_accounts"
print(f"\nWriting {table1}...")
sdf1 = spark.createDataFrame(players_df)
sdf1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table1)
count1 = spark.table(table1).count()
print(f"  {table1}: {count1:,} rows written")

# --- Write player_activities ---
table2 = f"{CATALOG}.{SCHEMA}.player_activities"
print(f"\nWriting {table2}...")
# Convert Int64 nullable to regular int with None for Spark compatibility
activities_pdf = activities_df.copy()
activities_pdf["opponent_id"] = activities_pdf["opponent_id"].astype("object")
sdf2 = spark.createDataFrame(activities_pdf)
sdf2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table2)
count2 = spark.table(table2).count()
print(f"  {table2}: {count2:,} rows written")

# --- Write next_best_actions ---
table3 = f"{CATALOG}.{SCHEMA}.next_best_actions"
print(f"\nWriting {table3}...")
sdf3 = spark.createDataFrame(nba_df)
sdf3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table3)
count3 = spark.table(table3).count()
print(f"  {table3}: {count3:,} rows written")

print(f"\n{'='*60}")
print(f"DONE - All 3 tables written to {CATALOG}.{SCHEMA}")
print(f"  player_accounts:  {count1:,} rows")
print(f"  player_activities: {count2:,} rows")
print(f"  next_best_actions: {count3:,} rows")
print(f"  Total: {count1 + count2 + count3:,} rows")
print(f"{'='*60}")

spark.stop()

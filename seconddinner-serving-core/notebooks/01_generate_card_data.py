"""
SecondDinner Serving Core — Step 1: Card Data Generation

Generates ~200 Marvel Snap card records with features, tags, and archetypes.
Output: otto_demo.snap_synergy.cards
"""
import subprocess, sys
try:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                    "dbldatagen==0.4.0.post1", "jmespath==1.0.1", "pyparsing==3.2.3"],
                   check=True)
except Exception:
    pass

import os

CATALOG = "otto_demo"
SCHEMA = "snap_synergy"
TABLE = f"{CATALOG}.{SCHEMA}.cards"

if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:
    from databricks.connect import DatabricksSession, DatabricksEnv
    env = (
        DatabricksEnv()
        .withDependencies("dbldatagen==0.4.0.post1")
        .withDependencies("jmespath==1.0.1")
        .withDependencies("pyparsing==3.2.5")
    )
    spark = DatabricksSession.builder.serverless(True).withEnvironment(env).getOrCreate()

from pyspark.sql import functions as F, types as T
import json

print(f"Spark version: {spark.version}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# --- Marvel Snap card data ---
# Real-ish cards with names, costs, powers, abilities, tags, and archetypes
CARDS = [
    ("Deadpool", 1, 1, "When this is destroyed, return it to your hand with double the Power.", "Destroy", "Destroy"),
    ("Wolverine", 2, 2, "When this is destroyed, regenerate it at a random location with +2 Power.", "Destroy", "Destroy"),
    ("Bucky Barnes", 1, 1, "When this is destroyed, create a Winter Soldier in its place.", "Destroy", "Destroy"),
    ("Nova", 1, 1, "When this is destroyed, give your other cards +1 Power.", "Destroy", "Destroy"),
    ("Carnage", 2, 2, "On Reveal: Destroy your other cards here. +2 Power for each destroyed.", "On Reveal,Destroy", "Destroy"),
    ("Venom", 3, 1, "On Reveal: Destroy your other cards here. Add their Power to this.", "On Reveal,Destroy", "Destroy"),
    ("Deathlok", 3, 5, "On Reveal: Destroy your other cards here.", "On Reveal,Destroy", "Destroy"),
    ("Killmonger", 3, 3, "On Reveal: Destroy all 1-Cost cards.", "On Reveal,Destroy", "Destroy"),
    ("Knull", 6, 0, "Ongoing: Has the combined Power of all cards destroyed this game.", "Ongoing,Destroy", "Destroy"),
    ("Death", 9, 12, "Costs 1 less for each card destroyed this game.", "Destroy", "Destroy"),
    ("Iron Man", 5, 0, "Ongoing: Double your total Power at this location.", "Ongoing", "Ongoing"),
    ("Blue Marvel", 5, 3, "Ongoing: Your other cards have +1 Power.", "Ongoing", "Ongoing"),
    ("Spectrum", 6, 5, "On Reveal: Give your Ongoing cards +2 Power.", "On Reveal,Ongoing", "Ongoing"),
    ("Patriot", 3, 1, "Ongoing: Your cards with no abilities have +2 Power.", "Ongoing", "Ongoing"),
    ("Ka-Zar", 4, 4, "Ongoing: Your 1-Cost cards have +1 Power.", "Ongoing", "Zoo"),
    ("Onslaught", 6, 7, "Ongoing: Double your other Ongoing effects at this location.", "Ongoing", "Ongoing"),
    ("Mystique", 3, 0, "On Reveal: If the last card you played has an Ongoing ability, copy it.", "On Reveal,Ongoing", "Ongoing"),
    ("Wong", 4, 2, "Ongoing: Your On Reveal abilities at this location trigger twice.", "Ongoing,On Reveal", "On Reveal"),
    ("Odin", 6, 8, "On Reveal: Activate the On Reveal abilities of your other cards here.", "On Reveal", "On Reveal"),
    ("White Tiger", 5, 1, "On Reveal: Add a 7-Power Tiger to another location.", "On Reveal", "On Reveal"),
    ("Ironheart", 3, 0, "On Reveal: Give 3 other friendly cards +2 Power.", "On Reveal", "On Reveal"),
    ("Silver Surfer", 3, 0, "On Reveal: Give your other 3-Cost cards +3 Power.", "On Reveal", "Surfer"),
    ("Sera", 5, 4, "Ongoing: Cards in your hand cost 1 less.", "Ongoing", "Surfer"),
    ("Brood", 3, 2, "On Reveal: Add 2 Broodlings to this location with the same Power.", "On Reveal", "Surfer"),
    ("Sebastian Shaw", 3, 4, "When any card is destroyed, this gains +2 Power.", "Destroy", "Destroy"),
    ("Hela", 6, 6, "On Reveal: Resurrect all cards you discarded from your hand.", "On Reveal,Discard", "Discard"),
    ("Apocalypse", 6, 8, "When you discard this, put it back with +4 Power.", "Discard", "Discard"),
    ("Morbius", 2, 0, "Ongoing: +2 Power for each time you discarded a card.", "Ongoing,Discard", "Discard"),
    ("Blade", 1, 3, "On Reveal: Discard a card from your hand.", "On Reveal,Discard", "Discard"),
    ("Lady Sif", 3, 4, "On Reveal: Discard the highest-cost card from your hand.", "On Reveal,Discard", "Discard"),
    ("Sword Master", 3, 6, "On Reveal: Discard a card from your hand.", "On Reveal,Discard", "Discard"),
    ("Colleen Wing", 2, 4, "On Reveal: Discard the lowest-cost card from your hand.", "On Reveal,Discard", "Discard"),
    ("Moon Knight", 3, 3, "On Reveal: Discard a card from each player's hand.", "On Reveal,Discard", "Discard"),
    ("Vulture", 3, 3, "When a card moves away from here, +5 Power.", "Move", "Move"),
    ("Dagger", 2, 1, "When this moves to a location, +3 Power for each enemy card there.", "Move", "Move"),
    ("Human Torch", 1, 2, "When this moves, double its Power.", "Move", "Move"),
    ("Kraven", 2, 2, "When a card moves here, +2 Power.", "Move", "Move"),
    ("Miles Morales", 4, 5, "Costs 1 less if a card moved last turn.", "Move", "Move"),
    ("Doctor Strange", 3, 3, "On Reveal: Move your highest-Power card to this location.", "On Reveal,Move", "Move"),
    ("Heimdall", 6, 8, "On Reveal: Move your other cards one location to the left.", "On Reveal,Move", "Move"),
    ("Angela", 2, 0, "When you play a card here, +2 Power.", "Ongoing", "Zoo"),
    ("Bishop", 3, 1, "When you play a card, +1 Power.", "Ongoing", "Zoo"),
    ("Squirrel Girl", 1, 1, "On Reveal: Add a 1-Power Squirrel to each other location.", "On Reveal", "Zoo"),
    ("Ant-Man", 1, 1, "Ongoing: If you have 3 other cards here, +3 Power.", "Ongoing", "Zoo"),
    ("Nightcrawler", 1, 2, "You can move this once.", "Move", "Zoo"),
    ("Sunspot", 1, 1, "At the end of each turn, gain +1 Power for each unspent Energy.", "Ongoing", "Zoo"),
    ("Iceman", 1, 2, "On Reveal: Increase the Cost of a random card in your opponent's hand by 1.", "On Reveal", "Control"),
    ("Scorpion", 2, 2, "On Reveal: Afflict cards in your opponent's hand with -1 Power.", "On Reveal", "Control"),
    ("Shang-Chi", 4, 3, "On Reveal: Destroy an enemy card with 9 or more Power at this location.", "On Reveal,Destroy", "Control"),
    ("Enchantress", 4, 4, "On Reveal: Remove the abilities from all Ongoing cards at this location.", "On Reveal", "Control"),
    ("Rogue", 3, 1, "On Reveal: Steal an Ongoing ability from an enemy card at this location.", "On Reveal", "Control"),
    ("Leech", 5, 3, "On Reveal: Remove the abilities from all cards in your opponent's hand.", "On Reveal", "Control"),
    ("Doctor Doom", 6, 5, "On Reveal: Add a 5-Power DoomBot to each other location.", "On Reveal", "Good Stuff"),
    ("America Chavez", 6, 9, "Always drawn on Turn 6. Doesn't appear before then.", "None", "Good Stuff"),
    ("Devil Dinosaur", 5, 3, "Ongoing: +2 Power for each card in your hand.", "Ongoing", "Devil Dino"),
    ("Moon Girl", 4, 4, "On Reveal: Duplicate your hand.", "On Reveal", "Devil Dino"),
    ("Mister Negative", 4, -1, "On Reveal: Swap the Cost and Power of all cards in your deck.", "On Reveal", "Negative"),
    ("Magik", 5, 3, "On Reveal: Change this location to Limbo. (Adds a turn.)", "On Reveal", "Negative"),
    ("Psylocke", 2, 1, "On Reveal: Next turn, you get +1 Energy.", "On Reveal", "Ramp"),
    ("Electro", 3, 2, "On Reveal: +1 Max Energy. Ongoing: You can only play 1 card a turn.", "On Reveal,Ongoing", "Ramp"),
    ("Wave", 3, 3, "On Reveal: All cards cost 4 until the end of next turn.", "On Reveal", "Ramp"),
    ("Galactus", 6, 3, "On Reveal: If this is your only card here, destroy the other locations.", "On Reveal,Destroy", "Galactus"),
    ("Spider-Man", 4, 3, "On Reveal: Your opponent can't play cards at this location next turn.", "On Reveal", "Control"),
    ("Magneto", 6, 12, "On Reveal: Move all enemy 3-Cost and 4-Cost cards to this location.", "On Reveal,Move", "Good Stuff"),
    ("Aero", 5, 8, "On Reveal: Move the last enemy card played this turn to this location.", "On Reveal,Move", "Control"),
    ("Lockjaw", 3, 2, "When you play a card here, swap it with a card from your deck.", "Ongoing", "Lockjaw"),
    ("Jubilee", 4, 1, "On Reveal: Play a card from your deck at this location.", "On Reveal", "Lockjaw"),
    ("Dracula", 4, 0, "At the end of the game, discard a card. This has its Power.", "Discard", "Discard"),
    ("Ebony Maw", 1, 7, "You can't play this after Turn 3. Ongoing: You can't play cards here.", "Ongoing", "Good Stuff"),
    ("The Hood", 1, -2, "On Reveal: Add a Demon to your hand.", "On Reveal", "Good Stuff"),
    ("Mysterio", 2, 4, "As you play this, play Illusions to the other locations.", "On Reveal", "Zoo"),
    ("Mojo", 2, 2, "Ongoing: If both players have 4 cards here, +6 Power.", "Ongoing", "Ongoing"),
    ("Cosmo", 3, 3, "Ongoing: On Reveal abilities won't happen at this location.", "Ongoing", "Control"),
    ("Armor", 2, 3, "Ongoing: Cards at this location can't be destroyed.", "Ongoing", "Ongoing"),
    ("Lizard", 2, 5, "Ongoing: -3 Power if your opponent has 4 cards here.", "Ongoing", "Zoo"),
    ("Warpath", 4, 5, "Ongoing: If any of your locations are empty, +4 Power.", "Ongoing", "Good Stuff"),
    ("Namor", 4, 5, "Ongoing: +5 Power if this is your only card here.", "Ongoing", "Good Stuff"),
    ("Taskmaster", 5, 0, "On Reveal: Set this card's Power equal to the Power of the last card you played.", "On Reveal", "Good Stuff"),
    ("Green Goblin", 3, -3, "On Reveal: Switch sides.", "On Reveal", "Junk"),
    ("Hobgoblin", 5, -8, "On Reveal: Switch sides.", "On Reveal", "Junk"),
    ("Debrii", 3, 3, "On Reveal: Add a Rock to each other location, for both players.", "On Reveal", "Junk"),
    ("Korg", 1, 2, "On Reveal: Shuffle a Rock into your opponent's deck.", "On Reveal", "Junk"),
    ("Rock Slide", 4, 6, "On Reveal: Shuffle 2 Rocks into your opponent's deck.", "On Reveal", "Junk"),
    ("Baron Mordo", 2, 3, "On Reveal: Your opponent draws a card. Set its Cost to 6.", "On Reveal", "Control"),
    ("Absorbing Man", 4, 3, "On Reveal: If the last card you played has an On Reveal, copy it.", "On Reveal", "On Reveal"),
    ("Maximus", 3, 7, "On Reveal: Your opponent draws 2 cards.", "On Reveal", "Good Stuff"),
    ("Typhoid Mary", 4, 10, "Ongoing: Your other cards have -1 Power.", "Ongoing", "Good Stuff"),
    ("Red Skull", 5, 15, "Ongoing: Your other cards at this location have -2 Power.", "Ongoing", "Good Stuff"),
    ("Sandman", 5, 4, "Ongoing: Both players can only play 1 card a turn.", "Ongoing", "Control"),
    ("Storm", 3, 2, "On Reveal: Flood this location. Next turn is the last turn cards can be played here.", "On Reveal", "Control"),
    ("Juggernaut", 3, 3, "On Reveal: If your opponent played cards here this turn, move them to other random locations.", "On Reveal,Move", "Control"),
    ("Polaris", 3, 5, "On Reveal: Move an enemy 1 or 2-Cost card to this location.", "On Reveal,Move", "Control"),
    ("Cloak", 2, 4, "On Reveal: Next turn, both players can move cards to this location.", "On Reveal,Move", "Move"),
    ("Ghost Spider", 1, 2, "On Reveal: Move the last card you played this turn to this location.", "On Reveal,Move", "Move"),
    ("Iron Fist", 1, 2, "On Reveal: Move the next card you play to the left.", "On Reveal,Move", "Move"),
    ("Multiple Man", 3, 3, "When this moves, add a copy to the old location.", "Move", "Move"),
    ("Vision", 5, 7, "You can move this each turn.", "Move", "Move"),
    ("Jeff", 2, 3, "You can move this once. Nothing can stop you from moving or playing this card.", "Move", "Move"),
    ("Captain Marvel", 5, 6, "At the end of the game, move to a location that wins you the game.", "Move", "Move"),
    ("Swarm", 2, 3, "When this is discarded, add two 0-Cost copies to your hand.", "Discard", "Discard"),
    ("Wolverine", 2, 2, "When this is destroyed, regenerate it at a random location with +2 Power.", "Destroy", "Destroy"),
    ("Nimrod", 5, 5, "When this is destroyed, add a copy to each other location.", "Destroy", "Destroy"),
    ("Arnim Zola", 6, 0, "On Reveal: Destroy a random friendly card here. Add copies to other locations.", "On Reveal,Destroy", "Destroy"),
    ("Wave", 3, 3, "On Reveal: All cards cost 4 until the end of next turn.", "On Reveal", "Ramp"),
    ("Zabu", 2, 2, "Ongoing: Your 4-Cost cards cost 1 less.", "Ongoing", "Zabu"),
    ("Darkhawk", 4, 1, "Ongoing: +2 Power for each card in your opponent's deck.", "Ongoing", "Darkhawk"),
    ("Black Widow", 2, 1, "On Reveal: Add a Widow's Bite to your opponent's hand.", "On Reveal", "Darkhawk"),
    ("Klaw", 5, 4, "Ongoing: +6 Power to the location to the right of this.", "Ongoing", "Ongoing"),
    ("Gamora", 5, 7, "On Reveal: If your opponent played a card here this turn, +5 Power.", "On Reveal", "Good Stuff"),
]

# Deduplicate by card_name (keep first occurrence)
seen = set()
unique_cards = []
for card in CARDS:
    if card[0] not in seen:
        seen.add(card[0])
        unique_cards.append(card)
CARDS = unique_cards

print(f"Generating {len(CARDS)} Marvel Snap cards → {TABLE}")

schema = T.StructType([
    T.StructField("card_name", T.StringType(), False),
    T.StructField("cost", T.IntegerType(), False),
    T.StructField("power", T.IntegerType(), False),
    T.StructField("ability_text", T.StringType(), False),
    T.StructField("tags", T.StringType(), False),
    T.StructField("archetype", T.StringType(), False),
])

df = spark.createDataFrame(CARDS, schema)

# Add derived features for clustering
df = (
    df
    .withColumn("tag_count", F.size(F.split(F.col("tags"), ",")))
    .withColumn("is_on_reveal", F.when(F.col("tags").contains("On Reveal"), 1).otherwise(0))
    .withColumn("is_ongoing", F.when(F.col("tags").contains("Ongoing"), 1).otherwise(0))
    .withColumn("is_destroy", F.when(F.col("tags").contains("Destroy"), 1).otherwise(0))
    .withColumn("is_move", F.when(F.col("tags").contains("Move"), 1).otherwise(0))
    .withColumn("is_discard", F.when(F.col("tags").contains("Discard"), 1).otherwise(0))
    .withColumn("power_cost_ratio", F.when(F.col("cost") > 0, F.col("power") / F.col("cost")).otherwise(F.col("power").cast("double")))
    .withColumn("ability_length", F.length(F.col("ability_text")))
)

# Write with CDF enabled (required for online table sync) and primary key
print(f"Writing to {TABLE}...")
df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").saveAsTable(TABLE)

# Add primary key constraint (idempotent)
spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN card_name SET NOT NULL")
try:
    spark.sql(f"ALTER TABLE {TABLE} ADD CONSTRAINT pk_card_name PRIMARY KEY (card_name)")
except Exception as e:
    if "ALREADY_EXISTS" in str(e):
        print("  PK constraint already exists, skipping.")
    else:
        raise

count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE}").collect()[0]["n"]
archetypes = spark.sql(f"SELECT archetype, COUNT(*) AS n FROM {TABLE} GROUP BY archetype ORDER BY n DESC").collect()
print(f"\n✓ Wrote {count} cards to {TABLE}")
print(f"  Archetypes:")
for row in archetypes:
    print(f"    {row['archetype']}: {row['n']}")

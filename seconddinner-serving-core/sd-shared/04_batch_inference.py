# Databricks notebook source
"""
SecondDinner Serving Core — Step 4: Batch Inference

Pre-computes synergy scores for all card pairs using tag overlap,
cluster membership, and cost-curve compatibility.
Output: <YOUR_CATALOG>.<YOUR_SCHEMA>.batch_synergy_scores
"""

# COMMAND ----------

# ── Configuration ────────────────────────────────────────────────────────────
# TODO: Set these to your catalog and schema
CATALOG = "<YOUR_CATALOG>"
SCHEMA = "<YOUR_SCHEMA>"
CARDS_TABLE = f"{CATALOG}.{SCHEMA}.cards"
CENTROIDS_TABLE = f"{CATALOG}.{SCHEMA}.cluster_centroids"
OUTPUT_TABLE = f"{CATALOG}.{SCHEMA}.batch_synergy_scores"

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

print(f"Computing batch synergy scores for all card pairs...")

cards = spark.table(CARDS_TABLE)
centroids = spark.table(CENTROIDS_TABLE).select("cluster_id", "cluster_name", "member_cards")

# Self-join to get all pairs
a = cards.alias("a")
b = cards.alias("b")

pairs = (
    a.crossJoin(b)
    .filter(F.col("a.card_name") < F.col("b.card_name"))
    .select(
        F.col("a.card_name").alias("card_a"),
        F.col("b.card_name").alias("card_b"),
        F.col("a.tags").alias("tags_a"),
        F.col("b.tags").alias("tags_b"),
        F.col("a.archetype").alias("arch_a"),
        F.col("b.archetype").alias("arch_b"),
        F.col("a.cost").alias("cost_a"),
        F.col("b.cost").alias("cost_b"),
        F.col("a.is_destroy").alias("destroy_a"),
        F.col("b.is_destroy").alias("destroy_b"),
        F.col("a.is_move").alias("move_a"),
        F.col("b.is_move").alias("move_b"),
        F.col("a.is_discard").alias("discard_a"),
        F.col("b.is_discard").alias("discard_b"),
        F.col("a.is_on_reveal").alias("on_reveal_a"),
        F.col("b.is_on_reveal").alias("on_reveal_b"),
        F.col("a.is_ongoing").alias("ongoing_a"),
        F.col("b.is_ongoing").alias("ongoing_b"),
    )
)

# COMMAND ----------

# Compute synergy score components
scored = pairs.withColumn(
    "same_archetype",
    F.when(F.col("arch_a") == F.col("arch_b"), 0.3).otherwise(0.0),
).withColumn(
    "tag_overlap",
    (
        F.col("destroy_a") * F.col("destroy_b") * 0.15
        + F.col("move_a") * F.col("move_b") * 0.15
        + F.col("discard_a") * F.col("discard_b") * 0.15
        + F.col("on_reveal_a") * F.col("on_reveal_b") * 0.1
        + F.col("ongoing_a") * F.col("ongoing_b") * 0.1
    ),
).withColumn(
    "cost_curve",
    F.when(F.abs(F.col("cost_a") - F.col("cost_b")) >= 3, 0.15)
    .when(F.abs(F.col("cost_a") - F.col("cost_b")) >= 2, 0.10)
    .when(F.abs(F.col("cost_a") - F.col("cost_b")) >= 1, 0.05)
    .otherwise(0.0),
).withColumn(
    "shared_tags",
    F.concat_ws(
        ",",
        F.when(F.col("destroy_a") + F.col("destroy_b") == 2, F.lit("Destroy")),
        F.when(F.col("move_a") + F.col("move_b") == 2, F.lit("Move")),
        F.when(F.col("discard_a") + F.col("discard_b") == 2, F.lit("Discard")),
        F.when(F.col("on_reveal_a") + F.col("on_reveal_b") == 2, F.lit("On Reveal")),
        F.when(F.col("ongoing_a") + F.col("ongoing_b") == 2, F.lit("Ongoing")),
    ),
).withColumn(
    "same_cluster",
    F.when(F.col("arch_a") == F.col("arch_b"), F.lit("true")).otherwise(F.lit("false")),
).withColumn(
    "synergy_score",
    F.col("same_archetype") + F.col("tag_overlap") + F.col("cost_curve"),
)

# Create both directions for easy lookup
forward = scored.select("card_a", "card_b", "synergy_score", "shared_tags", "same_cluster")
reverse = scored.select(
    F.col("card_b").alias("card_a"), F.col("card_a").alias("card_b"),
    "synergy_score", "shared_tags", "same_cluster",
)
result = forward.union(reverse)

# COMMAND ----------

print(f"Writing to {OUTPUT_TABLE}...")
result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE)

# Enable CDF and add primary key for Lakebase sync
spark.sql(f"ALTER TABLE {OUTPUT_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"ALTER TABLE {OUTPUT_TABLE} ALTER COLUMN card_a SET NOT NULL")
spark.sql(f"ALTER TABLE {OUTPUT_TABLE} ALTER COLUMN card_b SET NOT NULL")
try:
    spark.sql(f"ALTER TABLE {OUTPUT_TABLE} ADD CONSTRAINT pk_synergy PRIMARY KEY (card_a, card_b)")
except Exception as e:
    if "ALREADY_EXISTS" in str(e):
        print("  PK constraint already exists, skipping.")
    else:
        raise
print("  CDF enabled, PK (card_a, card_b) set — ready for Lakebase sync")

count = spark.sql(f"SELECT COUNT(*) AS n FROM {OUTPUT_TABLE}").collect()[0]["n"]
avg_score = spark.sql(f"SELECT AVG(synergy_score) AS avg FROM {OUTPUT_TABLE}").collect()[0]["avg"]
print(f"\n✓ Wrote {count:,} card pair scores to {OUTPUT_TABLE}")
print(f"  Average synergy score: {avg_score:.4f}")

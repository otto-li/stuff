# Databricks notebook source
"""
SecondDinner Serving Core — Step 2: Compute Card Embeddings

Uses Databricks Foundation Model embedding endpoint to embed card ability text.
Output: <YOUR_CATALOG>.<YOUR_SCHEMA>.card_embeddings
"""

# COMMAND ----------

# ── Configuration ────────────────────────────────────────────────────────────
# TODO: Set these to your catalog, schema, and embedding endpoint
CATALOG = "<YOUR_CATALOG>"
SCHEMA = "<YOUR_SCHEMA>"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.cards"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.card_embeddings"
EMBEDDING_ENDPOINT = "databricks-bge-large-en"  # Foundation Model API endpoint

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F, types as T
from openai import OpenAI
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Get workspace host + token for Foundation Model API
WORKSPACE_HOST = spark.conf.get("spark.databricks.workspaceUrl", "")
if WORKSPACE_HOST and not WORKSPACE_HOST.startswith("http"):
    WORKSPACE_HOST = f"https://{WORKSPACE_HOST}"
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # noqa

print(f"Computing embeddings for cards in {SOURCE_TABLE}")

# COMMAND ----------

# Load cards
cards_df = spark.table(SOURCE_TABLE).select("card_name", "ability_text").toPandas()
print(f"  Loaded {len(cards_df)} cards")

# Get embeddings via Foundation Model API (OpenAI-compatible)
client = OpenAI(api_key=TOKEN, base_url=f"{WORKSPACE_HOST}/serving-endpoints")

embeddings = []
BATCH_SIZE = 20
for i in range(0, len(cards_df), BATCH_SIZE):
    batch = cards_df.iloc[i : i + BATCH_SIZE]
    texts = batch["ability_text"].tolist()
    resp = client.embeddings.create(input=texts, model=EMBEDDING_ENDPOINT)
    for j, emb in enumerate(resp.data):
        embeddings.append({
            "card_name": batch.iloc[j]["card_name"],
            "ability_text": batch.iloc[j]["ability_text"],
            "embedding": emb.embedding,
        })
    print(f"  Embedded {min(i + BATCH_SIZE, len(cards_df))}/{len(cards_df)}")

print(f"  Embedding dimension: {len(embeddings[0]['embedding'])}")

# COMMAND ----------

# Convert to Spark DataFrame and write
embed_pdf = pd.DataFrame(embeddings)

schema = T.StructType([
    T.StructField("card_name", T.StringType(), False),
    T.StructField("ability_text", T.StringType(), False),
    T.StructField("embedding", T.ArrayType(T.FloatType()), False),
])

embed_df = spark.createDataFrame(embed_pdf, schema)

print(f"Writing to {TARGET_TABLE}...")
embed_df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").saveAsTable(TARGET_TABLE)

count = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_TABLE}").collect()[0]["n"]
print(f"\n✓ Wrote {count} card embeddings to {TARGET_TABLE}")

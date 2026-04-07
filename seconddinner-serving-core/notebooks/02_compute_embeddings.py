"""
SecondDinner Serving Core — Step 2: Compute Card Embeddings

Uses Databricks Foundation Model embedding endpoint to embed card ability text.
Output: otto_demo.snap_synergy.card_embeddings
"""
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "openai==1.82.0"], check=True)

import os

CATALOG = "otto_demo"
SCHEMA = "snap_synergy"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.cards"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.card_embeddings"
EMBEDDING_ENDPOINT = "databricks-bge-large-en"

if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    WORKSPACE_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()  # noqa
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # noqa
else:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", "fe-vm-otto-demo"],
        capture_output=True, text=True
    )
    TOKEN = result.stdout.strip()
    WORKSPACE_HOST = "https://fe-vm-otto-demo.cloud.databricks.com"

from openai import OpenAI
from pyspark.sql import functions as F, types as T

print(f"Computing embeddings for cards in {SOURCE_TABLE}")

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

# Convert to Spark DataFrame and write
import pandas as pd
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

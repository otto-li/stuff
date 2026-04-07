import { ServingType } from "./types";

export const CODE_EXAMPLES: Record<ServingType, string> = {
  batch: `# Batch Inference — Query pre-computed synergy scores from Delta
# These scores were computed offline by the batch pipeline

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read the pre-computed synergy scores table
synergy = spark.table("otto_demo.snap_synergy.batch_synergy_scores")

# Find top synergy partners for a card
top_partners = (
    synergy
    .filter("card_a = 'Deadpool'")
    .orderBy("synergy_score", ascending=False)
    .limit(10)
)
top_partners.show()

# Or via SQL warehouse (REST API)
import requests

resp = requests.post(
    "https://<workspace>/api/2.0/sql/statements",
    headers={"Authorization": "Bearer <token>"},
    json={
        "warehouse_id": "<warehouse_id>",
        "statement": """
            SELECT card_b, synergy_score, shared_tags
            FROM otto_demo.snap_synergy.batch_synergy_scores
            WHERE card_a = 'Deadpool'
            ORDER BY synergy_score DESC
            LIMIT 10
        """,
        "wait_timeout": "30s",
        "disposition": "INLINE",
    },
)
print(resp.json())`,

  model_serving: `# Model Serving — Real-time K-means cluster prediction
# The model is deployed as a Databricks serving endpoint

import requests

# Query the serving endpoint
resp = requests.post(
    "https://<workspace>/serving-endpoints/snap-card-clusterer/invocations",
    headers={
        "Authorization": "Bearer <token>",
        "Content-Type": "application/json",
    },
    json={
        "dataframe_records": [{"card_name": "Deadpool"}]
    },
)

result = resp.json()
# Returns: cluster assignment + centroids table
print(result["predictions"]["assignments"][0])
# → {"card_name": "Deadpool", "cluster_id": 7,
#    "cluster_name": "Destroy (6)", "cluster_members": [...]}

# Using the Databricks SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name="snap-card-clusterer",
    dataframe_records=[{"card_name": "Deadpool"}],
)
print(response.predictions)`,

  feature_serving: `# Feature Serving — Sub-ms card feature lookup via Lakebase
# Direct Postgres wire protocol for lowest latency

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
token = w.config.token  # or OAuth token

# Connect to Lakebase via Postgres wire protocol
conn = psycopg.connect(
    host="instance-<id>.database.cloud.databricks.com",
    port=443,
    dbname="default",
    user="token",
    password=token,
    sslmode="require",
)

# Sub-ms lookup by primary key
cur = conn.cursor()
cur.execute("""
    SELECT * FROM "otto_demo"."snap_synergy"."cards_online"
    WHERE card_name = %s
""", ("Deadpool",))

columns = [desc.name for desc in cur.description]
row = cur.fetchone()
print(dict(zip(columns, row)))
# → {"card_name": "Deadpool", "cost": 1, "power": 1,
#    "ability_text": "When this is destroyed...", ...}

conn.close()`,

  vector_search: `# Vector Search — Semantic similarity via embeddings
# Uses BGE-large-en embeddings + Databricks Vector Search

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Step 1: Embed the query text
from openai import OpenAI

client = OpenAI(
    api_key=w.config.token,
    base_url=f"{w.config.host}/serving-endpoints",
)

embedding = client.embeddings.create(
    input=["When this is destroyed, return it with double Power."],
    model="databricks-bge-large-en",
)
query_vector = embedding.data[0].embedding

# Step 2: Query the vector search index
import requests

resp = requests.post(
    f"{w.config.host}/api/2.0/vector-search/indexes/"
    "otto_demo.snap_synergy.card_embeddings_index/query",
    headers={"Authorization": f"Bearer {w.config.token}"},
    json={
        "query_vector": query_vector,
        "columns": ["card_name", "ability_text"],
        "num_results": 5,
    },
)
results = resp.json()
for row in results["result"]["data_array"]:
    print(row)  # [card_name, ability_text, score]`,

  foundation_model: `# Foundation Model API — LLM inference via AI Gateway
# Uses the OpenAI-compatible chat completions API

from openai import OpenAI
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
client = OpenAI(
    api_key=w.config.token,
    base_url=f"{w.config.host}/serving-endpoints",
)

response = client.chat.completions.create(
    model="databricks-claude-sonnet-4-6",
    messages=[
        {
            "role": "system",
            "content": "You are a Marvel Snap deck-building expert.",
        },
        {
            "role": "user",
            "content": "What cards synergize well with Deadpool?",
        },
    ],
    max_tokens=300,
    temperature=0.7,
)

print(response.choices[0].message.content)

# Streaming variant
stream = client.chat.completions.create(
    model="databricks-claude-sonnet-4-6",
    messages=[...],
    stream=True,
)
for chunk in stream:
    if chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="")`,

  retl: `# rETL — Reverse ETL from Delta to Lakebase for sub-ms queries
# Pattern: Batch compute → Delta table → Lakebase synced table → Postgres

# Step 1: Create a synced table (rETL pipeline)
import requests

resp = requests.post(
    "https://<workspace>/api/2.0/database/synced_tables",
    headers={"Authorization": "Bearer <token>"},
    json={
        "name": "otto_demo.snap_synergy.batch_synergy_online",
        "spec": {
            "source_table_full_name":
                "otto_demo.snap_synergy.batch_synergy_scores",
            "primary_key_columns": ["card_a", "card_b"],
            "scheduling_policy": "TRIGGERED",
        },
        "database_instance_name": "otto-demo-sd",
        "logical_database_name": "default",
    },
)
print(resp.json())

# Step 2: Query via Postgres wire protocol (sub-ms)
import psycopg

conn = psycopg.connect(
    host="ep-sweet-mud-d2q4etd3.database.us-east-1.cloud.databricks.com",
    port=443,
    dbname="default",
    user="token",
    password="<databricks-oauth-token>",
    sslmode="require",
)

cur = conn.cursor()
cur.execute("""
    SELECT card_b, synergy_score, shared_tags
    FROM "otto_demo"."snap_synergy"."batch_synergy_online"
    WHERE card_a = %s
    ORDER BY synergy_score DESC
    LIMIT 10
""", ("Deadpool",))

for row in cur.fetchall():
    print(row)
# → ('Wolverine', 0.55, 'Destroy')
# → ('Bucky Barnes', 0.55, 'Destroy')

conn.close()`,
};

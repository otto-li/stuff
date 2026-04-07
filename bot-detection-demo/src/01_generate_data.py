"""
Bot Detection Demo — Step 1: Synthetic Data Generation

Generates 500K web sessions with bot/human labels using dbldatagen.
Output: otto_demo.bot_detection.raw_traffic
"""
import subprocess, sys
# Install dependencies on serverless compute (non-fatal locally)
try:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                    "dbldatagen==0.4.0.post1", "jmespath==1.0.1", "pyparsing==3.2.3"],
                   check=True)
except Exception:
    pass  # deps already installed locally via uv

import os

CATALOG = "otto_demo"
SCHEMA = "bot_detection"
TABLE = f"{CATALOG}.{SCHEMA}.raw_traffic"
NUM_ROWS = 500_000

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

import dbldatagen as dg
from pyspark.sql import functions as F

print(f"Spark version: {spark.version}")
print(f"Generating {NUM_ROWS:,} web sessions → {TABLE}")

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# --- Define synthetic traffic schema ---
dataspec = (
    dg.DataGenerator(spark, name="bot_traffic", rows=NUM_ROWS, partitions=8)
    # Session identifier — derived from auto-id after build
    .withColumn("session_id", "long", minValue=1000000, maxValue=9999999)
    # Request metadata
    .withColumn("ip_octet1", "int", minValue=1, maxValue=254)
    .withColumn("ip_octet2", "int", minValue=0, maxValue=255)
    .withColumn("ip_octet3", "int", minValue=0, maxValue=255)
    .withColumn("ip_octet4", "int", minValue=0, maxValue=255)
    .withColumn("user_agent_type", "string",
                values=["Chrome/120", "Firefox/121", "Safari/17", "Python-urllib/3.11",
                        "curl/7.88", "Go-http-client/2.0", "Java/17.0", "Scrapy/2.11",
                        "Chrome/119", "Edge/120"],
                weights=[25, 15, 12, 8, 7, 6, 5, 5, 12, 5])
    .withColumn("device_type", "string",
                values=["desktop", "mobile", "tablet", "server", "unknown"],
                weights=[35, 30, 10, 15, 10])
    .withColumn("geo_country", "string",
                values=["US", "GB", "DE", "FR", "CN", "RU", "BR", "IN", "UA", "NL"],
                weights=[30, 8, 6, 5, 12, 10, 5, 5, 8, 11])
    # Behavioral signals — bots tend to be faster, more uniform, more requests
    .withColumn("num_requests", "int", minValue=1, maxValue=500)
    .withColumn("session_duration_secs", "double", minValue=0.5, maxValue=3600.0)
    .withColumn("avg_time_between_requests_ms", "double", minValue=10.0, maxValue=30000.0)
    .withColumn("page_views", "int", minValue=1, maxValue=100)
    .withColumn("click_count", "int", minValue=0, maxValue=200)
    .withColumn("mouse_events", "int", minValue=0, maxValue=5000)
    .withColumn("form_submissions", "int", minValue=0, maxValue=10)
    .withColumn("js_errors", "int", minValue=0, maxValue=50)
    .withColumn("referrer_type", "string",
                values=["direct", "search", "social", "programmatic", "none", "unknown"],
                weights=[20, 25, 15, 10, 20, 10])
    .withColumn("accept_language_set", "boolean", percentNulls=0.0)
    .withColumn("cookies_enabled", "boolean", percentNulls=0.0)
    .withColumn("tls_version", "string",
                values=["TLS1.3", "TLS1.2", "TLS1.1", "TLS1.0"],
                weights=[70, 25, 3, 2])
    # Label — ~25% bots overall, distributed across suspicious signals
    .withColumn("is_bot_raw", "int", values=[0, 1], weights=[75, 25])
    .withColumn("event_timestamp", "timestamp",
                begin="2024-01-01 00:00:00", end="2024-12-31 23:59:59",
                interval="1 second", random=True)
)

df = dataspec.build()

# Construct IP from octets and enrich label
df = df.withColumn(
    "ip_address",
    F.concat_ws(".", F.col("ip_octet1").cast("string"),
                F.col("ip_octet2").cast("string"),
                F.col("ip_octet3").cast("string"),
                F.col("ip_octet4").cast("string"))
).drop("ip_octet1", "ip_octet2", "ip_octet3", "ip_octet4")

# Make session_id a unique string (prefix long id)
df = df.withColumn("session_id", F.concat(F.lit("sess_"), F.col("session_id").cast("string")))

# Bias bots toward suspicious user agents (makes ML more realistic)
df = df.withColumn(
    "is_bot",
    F.when(
        F.col("user_agent_type").isin(["Python-urllib/3.11", "curl/7.88",
                                        "Go-http-client/2.0", "Java/17.0", "Scrapy/2.11"]),
        F.when(F.rand() < 0.80, F.lit(1)).otherwise(F.lit(0))
    ).when(
        F.col("avg_time_between_requests_ms") < 100,
        F.when(F.rand() < 0.70, F.lit(1)).otherwise(F.lit(0))
    ).otherwise(F.col("is_bot_raw"))
).drop("is_bot_raw", "id")

print(f"Writing to {TABLE}...")
df.write.format("delta").mode("overwrite").saveAsTable(TABLE)

count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE}").collect()[0]["n"]
bot_count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE} WHERE is_bot=1").collect()[0]["n"]
print(f"✓ Wrote {count:,} rows to {TABLE}")
print(f"  Bots: {bot_count:,} ({bot_count/count*100:.1f}%)")
print(f"  Humans: {count-bot_count:,} ({(count-bot_count)/count*100:.1f}%)")

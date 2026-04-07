"""
Bot Detection Demo — Step 2: Feature Engineering

Computes 20+ behavioral features from raw traffic data and stores
them in a Delta table with a primary key (required for online table sync).
Output: otto_demo.bot_detection.bot_features
"""
import os

CATALOG = "otto_demo"
SCHEMA = "bot_detection"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.raw_traffic"
FEATURES_TABLE = f"{CATALOG}.{SCHEMA}.bot_features"

if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

from pyspark.sql import functions as F

print(f"Spark version: {spark.version}")
print(f"Engineering features: {SOURCE_TABLE} → {FEATURES_TABLE}")

raw = spark.table(SOURCE_TABLE)
print(f"Source rows: {raw.count():,}")

# --- Compute behavioral features ---
features = raw.withColumns({
    # Request rate signals
    "requests_per_minute": F.when(
        F.col("session_duration_secs") > 0,
        F.col("num_requests") / (F.col("session_duration_secs") / 60.0)
    ).otherwise(F.lit(0.0)).cast("double"),

    "clicks_per_page": F.when(
        F.col("page_views") > 0,
        F.col("click_count") / F.col("page_views").cast("double")
    ).otherwise(F.lit(0.0)).cast("double"),

    # Speed signal — bots tend to be very fast (low ms between requests)
    "is_high_speed": F.when(F.col("avg_time_between_requests_ms") < 200, 1).otherwise(0).cast("int"),

    # User agent risk score
    "ua_risk_score": F.when(
        F.col("user_agent_type").isin(["Python-urllib/3.11", "Scrapy/2.11"]), 1.0
    ).when(
        F.col("user_agent_type").isin(["curl/7.88", "Go-http-client/2.0", "Java/17.0"]), 0.8
    ).when(
        F.col("user_agent_type").isin(["Chrome/120", "Chrome/119", "Firefox/121",
                                        "Safari/17", "Edge/120"]), 0.1
    ).otherwise(0.5).cast("double"),

    # Geographic risk — high-risk countries for invalid traffic
    "geo_risk_score": F.when(
        F.col("geo_country").isin(["CN", "RU", "UA"]), 0.7
    ).when(
        F.col("geo_country").isin(["US", "GB", "DE", "FR"]), 0.1
    ).otherwise(0.4).cast("double"),

    # Device type risk
    "device_risk_score": F.when(
        F.col("device_type").isin(["server", "unknown"]), 0.9
    ).when(
        F.col("device_type") == "desktop", 0.1
    ).when(
        F.col("device_type").isin(["mobile", "tablet"]), 0.15
    ).otherwise(0.5).cast("double"),

    # Mouse activity (bots rarely have mouse events)
    "has_mouse_activity": F.when(F.col("mouse_events") > 10, 1).otherwise(0).cast("int"),
    "mouse_events_per_click": F.when(
        F.col("click_count") > 0,
        F.col("mouse_events") / F.col("click_count").cast("double")
    ).otherwise(F.lit(0.0)).cast("double"),

    # JS execution proxy (errors suggest non-headless; no errors could mean no JS)
    "js_execution_score": F.when(F.col("js_errors") > 0, 0.3).otherwise(0.7).cast("double"),

    # Session depth
    "click_through_depth": F.when(
        F.col("page_views") > 0,
        F.col("page_views").cast("double") / F.greatest(F.col("num_requests").cast("double"), F.lit(1.0))
    ).otherwise(F.lit(0.0)).cast("double"),

    # Cookie / language header signals
    "missing_browser_signals": F.when(
        (~F.col("cookies_enabled")) | (~F.col("accept_language_set")), 1
    ).otherwise(0).cast("int"),

    # TLS downgrade risk
    "tls_risk": F.when(
        F.col("tls_version").isin(["TLS1.0", "TLS1.1"]), 0.8
    ).when(F.col("tls_version") == "TLS1.2", 0.3).otherwise(0.1).cast("double"),

    # Referrer type encoding
    "referrer_risk": F.when(
        F.col("referrer_type").isin(["programmatic", "none", "unknown"]), 0.6
    ).otherwise(0.2).cast("double"),

    # Composite bot score heuristic (for explainability)
    "heuristic_bot_score": (
        F.when(F.col("avg_time_between_requests_ms") < 200, 0.3).otherwise(0.0) +
        F.when(F.col("user_agent_type").isin(["Python-urllib/3.11", "Scrapy/2.11",
                                               "curl/7.88", "Go-http-client/2.0"]), 0.4).otherwise(0.0) +
        F.when(F.col("mouse_events") == 0, 0.15).otherwise(0.0) +
        F.when(~F.col("cookies_enabled"), 0.15).otherwise(0.0)
    ).cast("double"),
})

# Select final feature set (keep raw fields useful for model + label)
feature_cols = [
    "session_id",
    "event_timestamp",
    # Raw signals
    "num_requests",
    "session_duration_secs",
    "avg_time_between_requests_ms",
    "page_views",
    "click_count",
    "mouse_events",
    "form_submissions",
    "js_errors",
    # Computed features
    "requests_per_minute",
    "clicks_per_page",
    "is_high_speed",
    "ua_risk_score",
    "geo_risk_score",
    "device_risk_score",
    "has_mouse_activity",
    "mouse_events_per_click",
    "js_execution_score",
    "click_through_depth",
    "missing_browser_signals",
    "tls_risk",
    "referrer_risk",
    "heuristic_bot_score",
    # Categorical (kept for potential embedding)
    "user_agent_type",
    "device_type",
    "geo_country",
    "referrer_type",
    # Label
    "is_bot",
]

features_df = features.select(feature_cols)

print(f"Writing feature table with {len(feature_cols)} columns...")
(
    features_df.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")  # Required for online table sync
    .saveAsTable(FEATURES_TABLE)
)

# Add primary key constraint (required for Online Table) — idempotent
spark.sql(f"""
    ALTER TABLE {FEATURES_TABLE}
    ALTER COLUMN session_id SET NOT NULL
""")
try:
    spark.sql(f"""
        ALTER TABLE {FEATURES_TABLE} DROP CONSTRAINT IF EXISTS bot_features_pk
    """)
except Exception:
    pass
spark.sql(f"""
    ALTER TABLE {FEATURES_TABLE} ADD CONSTRAINT bot_features_pk
    PRIMARY KEY (session_id)
""")

count = spark.sql(f"SELECT COUNT(*) AS n FROM {FEATURES_TABLE}").collect()[0]["n"]
bot_pct = spark.sql(
    f"SELECT AVG(is_bot)*100 AS pct FROM {FEATURES_TABLE}"
).collect()[0]["pct"]
print(f"✓ Wrote {count:,} feature rows to {FEATURES_TABLE}")
print(f"  Bot rate: {bot_pct:.1f}%")
print(f"  Change Data Feed: enabled (required for online table sync)")
print(f"  Primary key: session_id")

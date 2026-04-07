"""
SecondDinner Serving Core — Step 3: Train K-Means Clustering Model

Trains a K-means model on card features and logs as a custom pyfunc that
returns BOTH cluster assignments AND the full centroids table.
This mirrors SecondDinner's real K-means use case.

Output: otto_demo.snap_synergy.card_clusterer (MLflow model)
        otto_demo.snap_synergy.cluster_centroids (Delta table)
"""
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                "scikit-learn==1.6.1", "mlflow==2.22.1", "pandas", "numpy"], check=True)

import os

CATALOG = "otto_demo"
SCHEMA = "snap_synergy"
CARDS_TABLE = f"{CATALOG}.{SCHEMA}.cards"
CENTROIDS_TABLE = f"{CATALOG}.{SCHEMA}.cluster_centroids"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.card_clusterer"
EXPERIMENT_NAME = "/Users/otto.li@databricks.com/seconddinner-serving-core"
N_CLUSTERS = 12

if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.serverless(True).getOrCreate()

import mlflow
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pickle
import json

print(f"Spark version: {spark.version}")
print(f"Loading cards from {CARDS_TABLE}...")

FEATURE_COLS = [
    "cost", "power", "tag_count",
    "is_on_reveal", "is_ongoing", "is_destroy", "is_move", "is_discard",
    "power_cost_ratio", "ability_length",
]

df = spark.table(CARDS_TABLE).select(["card_name", "archetype"] + FEATURE_COLS).toPandas()
print(f"Loaded {len(df)} cards")

X = df[FEATURE_COLS].fillna(0).values
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# --- Train K-Means ---
print(f"\nTraining K-Means with k={N_CLUSTERS}...")
kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init=10)
kmeans.fit(X_scaled)

df["cluster_id"] = kmeans.labels_

# Assign human-readable cluster names based on dominant archetype
cluster_names = {}
for cid in range(N_CLUSTERS):
    mask = df["cluster_id"] == cid
    dominant = df.loc[mask, "archetype"].mode()
    name = dominant.iloc[0] if len(dominant) > 0 else "Mixed"
    count = mask.sum()
    cluster_names[cid] = f"{name} ({count})"
    print(f"  Cluster {cid}: {cluster_names[cid]} — {df.loc[mask, 'card_name'].tolist()}")

# --- Build centroids table ---
centroids_df = pd.DataFrame(kmeans.cluster_centers_, columns=FEATURE_COLS)
centroids_df.insert(0, "cluster_id", range(N_CLUSTERS))
centroids_df.insert(1, "cluster_name", [cluster_names[i] for i in range(N_CLUSTERS)])

# Add member cards as JSON array
members = {}
for cid in range(N_CLUSTERS):
    members[cid] = df.loc[df["cluster_id"] == cid, "card_name"].tolist()
centroids_df["member_cards"] = [json.dumps(members[i]) for i in range(N_CLUSTERS)]

# Unscale centroids for readability
centroids_unscaled = scaler.inverse_transform(kmeans.cluster_centers_)
centroids_display = pd.DataFrame(centroids_unscaled, columns=FEATURE_COLS)
centroids_display.insert(0, "cluster_id", range(N_CLUSTERS))
centroids_display.insert(1, "cluster_name", [cluster_names[i] for i in range(N_CLUSTERS)])
centroids_display["member_cards"] = [json.dumps(members[i]) for i in range(N_CLUSTERS)]

# Save centroids to Delta
print(f"\nWriting centroids to {CENTROIDS_TABLE}...")
spark_centroids = spark.createDataFrame(centroids_display)
spark_centroids.write.format("delta").mode("overwrite").saveAsTable(CENTROIDS_TABLE)

# --- Log as custom pyfunc ---
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_NAME)

# Save model artifacts to temp files
import tempfile
tmpdir = tempfile.mkdtemp()
pickle.dump(kmeans, open(f"{tmpdir}/kmeans.pkl", "wb"))
pickle.dump(scaler, open(f"{tmpdir}/scaler.pkl", "wb"))
json.dump(FEATURE_COLS, open(f"{tmpdir}/feature_cols.json", "w"))
json.dump(cluster_names, open(f"{tmpdir}/cluster_names.json", "w"))
json.dump({str(k): v for k, v in members.items()}, open(f"{tmpdir}/cluster_members.json", "w"))
centroids_display.to_json(f"{tmpdir}/centroids.json", orient="records")


class CardClustererModel(mlflow.pyfunc.PythonModel):
    """Custom pyfunc that returns cluster assignment + full centroids table."""

    def load_context(self, context):
        self.kmeans = pickle.load(open(context.artifacts["kmeans"], "rb"))
        self.scaler = pickle.load(open(context.artifacts["scaler"], "rb"))
        self.feature_cols = json.load(open(context.artifacts["feature_cols"]))
        self.cluster_names = json.load(open(context.artifacts["cluster_names"]))
        self.cluster_members = json.load(open(context.artifacts["cluster_members"]))
        self.centroids = json.load(open(context.artifacts["centroids"]))

    def predict(self, context, model_input):
        import pandas as pd
        import numpy as np

        # model_input may have card_name + features, or just features
        if "card_name" in model_input.columns:
            card_names = model_input["card_name"].tolist()
            # Look up card by name from centroids members
            # For now, return the cluster membership info
            results = []
            for name in card_names:
                found = False
                for cid_str, member_list in self.cluster_members.items():
                    if name in member_list:
                        cid = int(cid_str)
                        results.append({
                            "card_name": name,
                            "cluster_id": cid,
                            "cluster_name": self.cluster_names[str(cid)],
                            "cluster_members": member_list,
                        })
                        found = True
                        break
                if not found:
                    results.append({
                        "card_name": name,
                        "cluster_id": -1,
                        "cluster_name": "Unknown",
                        "cluster_members": [],
                    })
            return {"assignments": results, "centroids": self.centroids}

        # Feature-vector input: run actual K-means prediction
        features = model_input[self.feature_cols].fillna(0).values
        scaled = self.scaler.transform(features)
        labels = self.kmeans.predict(scaled)
        distances = np.min(
            self.kmeans.transform(scaled), axis=1
        )

        assignments = []
        for i, (label, dist) in enumerate(zip(labels, distances)):
            cid = int(label)
            assignments.append({
                "cluster_id": cid,
                "cluster_name": self.cluster_names[str(cid)],
                "distance": float(dist),
                "cluster_members": self.cluster_members[str(cid)],
            })

        return {"assignments": assignments, "centroids": self.centroids}


with mlflow.start_run(run_name="kmeans-card-clusterer") as run:
    mlflow.log_params({"n_clusters": N_CLUSTERS, "n_cards": len(df), "features": len(FEATURE_COLS)})
    mlflow.log_metric("inertia", kmeans.inertia_)

    # Create input example
    input_example = pd.DataFrame([{"card_name": "Deadpool"}])

    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=CardClustererModel(),
        artifacts={
            "kmeans": f"{tmpdir}/kmeans.pkl",
            "scaler": f"{tmpdir}/scaler.pkl",
            "feature_cols": f"{tmpdir}/feature_cols.json",
            "cluster_names": f"{tmpdir}/cluster_names.json",
            "cluster_members": f"{tmpdir}/cluster_members.json",
            "centroids": f"{tmpdir}/centroids.json",
        },
        input_example=input_example,
        registered_model_name=MODEL_NAME,
        pip_requirements=["scikit-learn==1.6.1", "pandas", "numpy"],
    )

    print(f"\n✓ Model registered: {MODEL_NAME}")
    print(f"  Run ID: {run.info.run_id}")
    print(f"  Inertia: {kmeans.inertia_:.2f}")

# Tag as champion
from mlflow.tracking import MlflowClient
client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
client.set_model_version_tag(MODEL_NAME, latest.version, "stage", "champion")
print(f"  Version {latest.version} tagged as champion")
print(f"\nDone! {N_CLUSTERS} clusters, {len(df)} cards, centroids table written to {CENTROIDS_TABLE}")

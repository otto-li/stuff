# Databricks notebook source
"""
SecondDinner Serving Core — Step 6: Create Model Serving Endpoint

Deploys the K-means card clusterer pyfunc to a Databricks model serving
endpoint for real-time cluster assignment + centroids table.

Output: serving endpoint '<YOUR_ENDPOINT_NAME>'
"""

# COMMAND ----------

# ── Configuration ────────────────────────────────────────────────────────────
# TODO: Set these to your catalog, schema, and endpoint name
CATALOG = "<YOUR_CATALOG>"
SCHEMA = "<YOUR_SCHEMA>"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.card_clusterer"
ENDPOINT_NAME = "<YOUR_ENDPOINT_NAME>"  # e.g. "snap-card-clusterer"

# COMMAND ----------

%pip install mlflow

# COMMAND ----------

import os
import time
import json
import requests
import mlflow

mlflow.set_registry_uri("databricks-uc")
from mlflow.tracking import MlflowClient

# Resolve workspace host + token
WORKSPACE_HOST = spark.conf.get("spark.databricks.workspaceUrl", "")
if WORKSPACE_HOST and not WORKSPACE_HOST.startswith("http"):
    WORKSPACE_HOST = f"https://{WORKSPACE_HOST}"
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # noqa
BASE_URL = WORKSPACE_HOST.rstrip("/")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# COMMAND ----------

# --- Find latest model version ---
client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if not versions:
    raise RuntimeError(f"No versions found for {MODEL_NAME}. Run 03_train_kmeans first.")

latest_version = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
print(f"Using model: {MODEL_NAME} version {latest_version.version}")


def get_endpoint(name):
    resp = requests.get(f"{BASE_URL}/api/2.0/serving-endpoints/{name}", headers=HEADERS)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def wait_for_endpoint_ready(name, timeout_secs=900):
    start = time.time()
    while True:
        ep = get_endpoint(name)
        state = ep.get("state", {}).get("ready", "NOT_READY")
        print(f"  State: {state}")
        if state == "READY":
            return ep
        if state == "FAILED":
            raise RuntimeError(f"Endpoint failed: {json.dumps(ep.get('state'), indent=2)}")
        if time.time() - start > timeout_secs:
            raise TimeoutError(f"Timed out waiting for endpoint (state={state})")
        time.sleep(30)

# COMMAND ----------

config = {
    "served_entities": [
        {
            "name": "card-clusterer-v1",
            "entity_name": MODEL_NAME,
            "entity_version": str(latest_version.version),
            "scale_to_zero_enabled": True,
            "workload_size": "Small",
        }
    ],
    "traffic_config": {
        "routes": [
            {
                "served_model_name": "card-clusterer-v1",
                "traffic_percentage": 100,
            }
        ]
    },
}

ai_gateway = {
    "inference_table_config": {
        "catalog_name": CATALOG,
        "schema_name": SCHEMA,
        "table_name_prefix": "clusterer_inference",
        "enabled": True,
    },
    "usage_tracking_config": {"enabled": True},
}

existing = get_endpoint(ENDPOINT_NAME)
if existing:
    state = existing.get("state", {}).get("config_update", "NOT_UPDATING")
    if state == "IN_PROGRESS":
        print(f"Endpoint has a pending config update — waiting...")
        wait_for_endpoint_ready(ENDPOINT_NAME)
    print(f"Endpoint '{ENDPOINT_NAME}' exists — updating config...")
    resp = requests.put(f"{BASE_URL}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/config", headers=HEADERS, json=config)
    if resp.status_code == 409:
        print("  Config update conflict — skipping, endpoint already configured.")
    else:
        resp.raise_for_status()
        requests.put(f"{BASE_URL}/api/2.0/serving-endpoints/{ENDPOINT_NAME}/ai-gateway", headers=HEADERS, json=ai_gateway)
else:
    print(f"Creating endpoint '{ENDPOINT_NAME}'...")
    resp = requests.post(
        f"{BASE_URL}/api/2.0/serving-endpoints",
        headers=HEADERS,
        json={"name": ENDPOINT_NAME, "config": config, "ai_gateway": ai_gateway},
    )
    resp.raise_for_status()

print("Waiting for endpoint to be ready...")
endpoint = wait_for_endpoint_ready(ENDPOINT_NAME)

endpoint_url = f"{BASE_URL}/serving-endpoints/{ENDPOINT_NAME}/invocations"
print(f"\n✓ Model serving endpoint ready!")
print(f"  Name: {ENDPOINT_NAME}")
print(f"  URL: {endpoint_url}")
print(f"  Model: {MODEL_NAME} v{latest_version.version}")

# COMMAND ----------

# --- Test inference ---
print("=== Running test inference ===")
test_payload = {"dataframe_records": [{"card_name": "Deadpool"}]}
resp = requests.post(endpoint_url, headers=HEADERS, json=test_payload)
resp.raise_for_status()
result = resp.json()
print(f"Test result: {json.dumps(result, indent=2)}")

"""
SecondDinner Serving Core — Step 6: Create Model Serving Endpoint

Deploys the K-means card clusterer pyfunc to a Databricks model serving
endpoint for real-time cluster assignment + centroids table.

Output: serving endpoint 'snap-card-clusterer'
"""
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "mlflow==2.22.1"], check=True)

import os
import time
import json
import requests

CATALOG = "otto_demo"
SCHEMA = "snap_synergy"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.card_clusterer"
ENDPOINT_NAME = "snap-card-clusterer"

# --- Resolve workspace host + token ---
if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    WORKSPACE_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()  # noqa
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # noqa
else:
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", "fe-vm-otto-demo"],
        capture_output=True, text=True
    )
    TOKEN = result.stdout.strip()
    WORKSPACE_HOST = "https://fe-vm-otto-demo.cloud.databricks.com"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}
if WORKSPACE_HOST and not WORKSPACE_HOST.startswith("http"):
    WORKSPACE_HOST = "https://" + WORKSPACE_HOST
BASE_URL = WORKSPACE_HOST.rstrip("/")

# --- Find latest model version ---
import mlflow
mlflow.set_registry_uri("databricks-uc")
from mlflow.tracking import MlflowClient
client = MlflowClient()

versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if not versions:
    raise RuntimeError(f"No versions found for {MODEL_NAME}. Run 03_train_kmeans.py first.")

latest_version = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
print(f"Using model: {MODEL_NAME} version {latest_version.version}")


def get_endpoint(name: str) -> dict | None:
    resp = requests.get(f"{BASE_URL}/api/2.0/serving-endpoints/{name}", headers=HEADERS)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def create_or_update_endpoint(name: str, model_name: str, version: str) -> dict:
    config = {
        "served_entities": [
            {
                "name": "card-clusterer-v1",
                "entity_name": model_name,
                "entity_version": version,
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

    existing = get_endpoint(name)
    if existing:
        # Wait for any pending config update to finish before updating
        state = existing.get("state", {}).get("config_update", "NOT_UPDATING")
        if state == "IN_PROGRESS":
            print(f"Endpoint '{name}' has a pending config update — waiting...")
            wait_for_endpoint_ready(name)
        print(f"Endpoint '{name}' exists — updating config...")
        resp = requests.put(
            f"{BASE_URL}/api/2.0/serving-endpoints/{name}/config",
            headers=HEADERS, json=config,
        )
        if resp.status_code == 409:
            print("  Config update conflict (pending update) — skipping, endpoint already configured.")
            return existing
        resp.raise_for_status()
        print("  Updating AI Gateway config...")
        gw_resp = requests.put(
            f"{BASE_URL}/api/2.0/serving-endpoints/{name}/ai-gateway",
            headers=HEADERS, json=ai_gateway,
        )
        if not gw_resp.ok:
            print(f"  AI Gateway update: {gw_resp.text}")
    else:
        print(f"Creating endpoint '{name}'...")
        resp = requests.post(
            f"{BASE_URL}/api/2.0/serving-endpoints",
            headers=HEADERS,
            json={"name": name, "config": config, "ai_gateway": ai_gateway},
        )
        resp.raise_for_status()
    return resp.json()


def wait_for_endpoint_ready(name: str, timeout_secs: int = 900) -> dict:
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


# --- Create/update endpoint ---
create_or_update_endpoint(ENDPOINT_NAME, MODEL_NAME, str(latest_version.version))
print("Waiting for endpoint to be ready (5-10 minutes)...")
endpoint = wait_for_endpoint_ready(ENDPOINT_NAME)

endpoint_url = f"{BASE_URL}/serving-endpoints/{ENDPOINT_NAME}/invocations"
print(f"\n✓ Model serving endpoint ready!")
print(f"  Name: {ENDPOINT_NAME}")
print(f"  URL: {endpoint_url}")
print(f"  Model: {MODEL_NAME} v{latest_version.version}")

# --- Test inference ---
print("\n=== Running test inference ===")
test_payload = {"dataframe_records": [{"card_name": "Deadpool"}]}
resp = requests.post(endpoint_url, headers=HEADERS, json=test_payload)
resp.raise_for_status()
result = resp.json()
print(f"Test result: {json.dumps(result, indent=2)}")

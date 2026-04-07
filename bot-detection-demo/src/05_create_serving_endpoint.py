"""
Bot Detection Demo — Step 5: Create Model Serving Endpoint

Creates a Databricks model serving endpoint for real-time bot detection.
The endpoint uses the registered XGBoost model and can optionally perform
feature lookups from the Lakebase online table.

Output: serving endpoint 'bot-detector-endpoint'
"""
import subprocess, sys
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "mlflow==2.22.1"],
               check=True)

import os
import time
import json
import requests

CATALOG = "otto_demo"
SCHEMA = "bot_detection"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.bot_detector"
ENDPOINT_NAME = "bot-detector-endpoint"

# --- Resolve workspace host + token ---
if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    WORKSPACE_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()  # noqa
    TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # noqa
else:
    import subprocess
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

# --- Find the latest registered model version ---
import mlflow
mlflow.set_registry_uri("databricks-uc")
from mlflow.tracking import MlflowClient
client = MlflowClient()

versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if not versions:
    raise RuntimeError(f"No versions found for model {MODEL_NAME}. Run 03_train_model.py first.")

latest_version = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
print(f"Using model: {MODEL_NAME} version {latest_version.version}")


def get_endpoint(name: str) -> dict | None:
    resp = requests.get(
        f"{BASE_URL}/api/2.0/serving-endpoints/{name}",
        headers=HEADERS,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def create_or_update_endpoint(name: str, model_name: str, version: str) -> dict:
    config = {
        "served_entities": [
            {
                "name": "bot-detector-v1",
                "entity_name": model_name,
                "entity_version": version,
                "scale_to_zero_enabled": True,
                "workload_size": "Small",
            }
        ],
        "traffic_config": {
            "routes": [
                {
                    "served_model_name": "bot-detector-v1",
                    "traffic_percentage": 100,
                }
            ]
        },
    }

    # AI Gateway config — replaces legacy auto_capture_config
    ai_gateway = {
        "inference_table_config": {
            "catalog_name": "otto_demo",
            "schema_name": "bot_detection",
            "table_name_prefix": "bot_inference",
            "enabled": True,
        },
        "usage_tracking_config": {
            "enabled": True,
        },
    }

    existing = get_endpoint(name)
    if existing:
        print(f"Endpoint '{name}' exists — updating config...")
        resp = requests.put(
            f"{BASE_URL}/api/2.0/serving-endpoints/{name}/config",
            headers=HEADERS,
            json=config,
        )
        resp.raise_for_status()
        # Update AI Gateway separately
        print("  Updating AI Gateway config...")
        gw_resp = requests.put(
            f"{BASE_URL}/api/2.0/serving-endpoints/{name}/ai-gateway",
            headers=HEADERS,
            json=ai_gateway,
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
        config_state = ep.get("pending_config", {}).get("config_version")
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
print("Waiting for endpoint to be ready (this takes 5-10 minutes)...")
endpoint = wait_for_endpoint_ready(ENDPOINT_NAME)

endpoint_url = f"{BASE_URL}/serving-endpoints/{ENDPOINT_NAME}/invocations"
print(f"\n✓ Model serving endpoint ready!")
print(f"  Name: {ENDPOINT_NAME}")
print(f"  URL: {endpoint_url}")
print(f"  Model: {MODEL_NAME} v{latest_version.version}")
print(f"  Scale-to-zero: enabled")

# --- Test inference ---
print("\n=== Running test inference ===")
test_payload = {
    "dataframe_records": [
        {
            "num_requests": 850,
            "session_duration_secs": 12.5,
            "avg_time_between_requests_ms": 45.0,
            "page_views": 2,
            "click_count": 0,
            "mouse_events": 0,
            "form_submissions": 0,
            "js_errors": 0,
            "requests_per_minute": 408.0,
            "clicks_per_page": 0.0,
            "is_high_speed": 1,
            "ua_risk_score": 1.0,
            "geo_risk_score": 0.7,
            "device_risk_score": 0.9,
            "has_mouse_activity": 0,
            "mouse_events_per_click": 0.0,
            "js_execution_score": 0.7,
            "click_through_depth": 0.002,
            "missing_browser_signals": 1,
            "tls_risk": 0.3,
            "referrer_risk": 0.6,
            "heuristic_bot_score": 0.85,
        },
        {
            "num_requests": 12,
            "session_duration_secs": 185.0,
            "avg_time_between_requests_ms": 15420.0,
            "page_views": 8,
            "click_count": 23,
            "mouse_events": 1847,
            "form_submissions": 1,
            "js_errors": 2,
            "requests_per_minute": 3.9,
            "clicks_per_page": 2.875,
            "is_high_speed": 0,
            "ua_risk_score": 0.1,
            "geo_risk_score": 0.1,
            "device_risk_score": 0.1,
            "has_mouse_activity": 1,
            "mouse_events_per_click": 80.3,
            "js_execution_score": 0.3,
            "click_through_depth": 0.67,
            "missing_browser_signals": 0,
            "tls_risk": 0.1,
            "referrer_risk": 0.2,
            "heuristic_bot_score": 0.0,
        },
    ]
}

resp = requests.post(endpoint_url, headers=HEADERS, json=test_payload)
resp.raise_for_status()
predictions = resp.json()
print(f"Test results (1=bot, 0=human):")
print(f"  Session 1 (bot-like):   {predictions}")
print(f"\nFull response: {json.dumps(predictions, indent=2)}")

"""
SecondDinner Serving Core — Step 7: Vector Search Index

Creates a Vector Search endpoint and Delta Sync index on card embeddings
for semantic similarity search.

Output: Vector Search index otto_demo.snap_synergy.card_embeddings_index
"""
import os
import time
import json
import requests

CATALOG = "otto_demo"
SCHEMA = "snap_synergy"
EMBEDDINGS_TABLE = f"{CATALOG}.{SCHEMA}.card_embeddings"
VS_ENDPOINT = "snap-vs-endpoint"
VS_INDEX = f"{CATALOG}.{SCHEMA}.card_embeddings_index"

# --- Resolve workspace host + token ---
if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
    WORKSPACE_HOST = os.environ.get("DATABRICKS_HOST", "")
    TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
    if not WORKSPACE_HOST:
        WORKSPACE_HOST = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()  # noqa
    if not TOKEN:
        TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()  # noqa
else:
    import subprocess
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", "fe-vm-otto-demo"],
        capture_output=True, text=True
    )
    TOKEN = result.stdout.strip()
    WORKSPACE_HOST = "https://fe-vm-otto-demo.cloud.databricks.com"

if WORKSPACE_HOST and not WORKSPACE_HOST.startswith("http"):
    WORKSPACE_HOST = "https://" + WORKSPACE_HOST
BASE_URL = WORKSPACE_HOST.rstrip("/")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

print(f"Workspace: {WORKSPACE_HOST}")
print(f"Embeddings table: {EMBEDDINGS_TABLE}")
print(f"VS endpoint: {VS_ENDPOINT}")
print(f"VS index: {VS_INDEX}")


# --- Step 1: Create Vector Search endpoint (if not exists) ---
print("\nStep 1: Ensuring Vector Search endpoint exists...")
resp = requests.get(f"{BASE_URL}/api/2.0/vector-search/endpoints/{VS_ENDPOINT}", headers=HEADERS)
if resp.status_code == 404:
    print(f"  Creating endpoint {VS_ENDPOINT}...")
    create_resp = requests.post(
        f"{BASE_URL}/api/2.0/vector-search/endpoints",
        headers=HEADERS,
        json={"name": VS_ENDPOINT, "endpoint_type": "STANDARD"},
    )
    if not create_resp.ok:
        print(f"  Response: {create_resp.text}")
    create_resp.raise_for_status()
    print("  Endpoint created, waiting for ONLINE status...")

    # Wait for endpoint to be online
    start = time.time()
    while time.time() - start < 600:
        check = requests.get(f"{BASE_URL}/api/2.0/vector-search/endpoints/{VS_ENDPOINT}", headers=HEADERS)
        check.raise_for_status()
        status = check.json().get("endpoint_status", {}).get("state", "UNKNOWN")
        print(f"  VS endpoint state: {status}")
        if status == "ONLINE":
            break
        time.sleep(30)
else:
    resp.raise_for_status()
    status = resp.json().get("endpoint_status", {}).get("state", "UNKNOWN")
    print(f"  Endpoint already exists (state={status})")


# --- Step 2: Create Delta Sync index ---
print(f"\nStep 2: Creating Delta Sync index {VS_INDEX}...")

# Check if index exists
resp = requests.get(f"{BASE_URL}/api/2.0/vector-search/indexes/{VS_INDEX}", headers=HEADERS)
if resp.status_code == 200:
    print(f"  Index already exists")
    index_info = resp.json()
else:
    print(f"  Creating new index...")
    create_payload = {
        "name": VS_INDEX,
        "endpoint_name": VS_ENDPOINT,
        "primary_key": "card_name",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": EMBEDDINGS_TABLE,
            "embedding_vector_columns": [
                {
                    "name": "embedding",
                    "embedding_dimension": 1024,  # BGE-large-en dimension
                }
            ],
            "pipeline_type": "TRIGGERED",
        },
    }
    create_resp = requests.post(
        f"{BASE_URL}/api/2.0/vector-search/indexes",
        headers=HEADERS,
        json=create_payload,
    )
    if not create_resp.ok:
        print(f"  Response: {create_resp.text}")
    create_resp.raise_for_status()
    print("  Index created")

# Wait for index to be ready
print("  Waiting for index sync...")
start = time.time()
while time.time() - start < 600:
    check = requests.get(f"{BASE_URL}/api/2.0/vector-search/indexes/{VS_INDEX}", headers=HEADERS)
    check.raise_for_status()
    info = check.json()
    status = info.get("status", {}).get("ready", False)
    message = info.get("status", {}).get("message", "")
    print(f"  Index ready: {status} ({message})")
    if status:
        break
    time.sleep(30)

print(f"\n✓ Vector Search index ready!")
print(f"  Endpoint: {VS_ENDPOINT}")
print(f"  Index: {VS_INDEX}")
print(f"  Source: {EMBEDDINGS_TABLE}")

# --- Test query ---
print("\n=== Running test query ===")
test_resp = requests.post(
    f"{BASE_URL}/api/2.0/vector-search/indexes/{VS_INDEX}/query",
    headers=HEADERS,
    json={
        "query_text": "Deadpool",
        "columns": ["card_name", "ability_text"],
        "num_results": 5,
    },
)
if test_resp.ok:
    results = test_resp.json()
    print(f"  Results: {json.dumps(results.get('result', {}).get('data_array', []), indent=2)}")
else:
    print(f"  Test query failed (index may still be syncing): {test_resp.text}")

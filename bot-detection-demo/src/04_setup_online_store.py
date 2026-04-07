"""
Bot Detection Demo — Step 4: Online Feature Store (Lakebase Synced Table)

Creates the Lakebase online catalog and a Synced Table that syncs from
the offline Delta feature store into Lakebase PostgreSQL for sub-millisecond
feature lookup at model serving time.

Architecture:
  otto_demo.bot_detection.bot_features (Delta/offline store)
      ↓ continuous sync
  demo-database (Lakebase PostgreSQL CU_1 instance)
      ↕
  bot_detection_online catalog (Unity Catalog → Lakebase)
      └── public.bot_features_online  (Synced Table / online feature store)

Output: bot_detection_online.public.bot_features_online
"""
import os
import time
import json
import requests

SOURCE_TABLE = "otto_demo.bot_detection.bot_features"
DB_INSTANCE = "demo-database"
LOGICAL_DB = "bot_detection_db"
ONLINE_CATALOG = "otto_demo"
SYNCED_TABLE = "otto_demo.bot_detection.bot_features_online"

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
print(f"Source Delta table: {SOURCE_TABLE}")
print(f"Lakebase instance: {DB_INSTANCE}")
print(f"Online catalog: {ONLINE_CATALOG}")
print(f"Synced table: {SYNCED_TABLE}")


def wait_for_instance(instance_name: str, timeout_secs: int = 300) -> dict:
    start = time.time()
    while True:
        resp = requests.get(f"{BASE_URL}/api/2.0/database/instances/{instance_name}", headers=HEADERS)
        resp.raise_for_status()
        inst = resp.json()
        state = inst.get("state", "UNKNOWN")
        print(f"  Lakebase instance state: {state}")
        if state == "AVAILABLE":
            return inst
        if state in ("FAILED",):
            raise RuntimeError(f"Lakebase instance failed: {state}")
        if time.time() - start > timeout_secs:
            raise TimeoutError(f"Instance not available after {timeout_secs}s")
        time.sleep(20)


def ensure_instance_running(instance_name: str) -> dict:
    resp = requests.get(f"{BASE_URL}/api/2.0/database/instances/{instance_name}", headers=HEADERS)
    resp.raise_for_status()
    inst = resp.json()
    state = inst.get("state", "UNKNOWN")
    if state == "STOPPED":
        print(f"  Starting stopped instance {instance_name}...")
        patch = requests.patch(
            f"{BASE_URL}/api/2.0/database/instances/{instance_name}",
            headers=HEADERS,
            json={"stopped": False}
        )
        patch.raise_for_status()
    return wait_for_instance(instance_name)


def ensure_online_catalog(catalog_name: str, instance_name: str, db_name: str) -> None:
    # Check if catalog exists
    resp = requests.get(
        f"{BASE_URL}/api/2.0/unity-catalog/catalogs/{catalog_name}",
        headers=HEADERS
    )
    if resp.status_code == 404 or resp.status_code == 403:
        print(f"  Creating online catalog {catalog_name}...")
        create = requests.post(
            f"{BASE_URL}/api/2.0/database/catalogs",
            headers=HEADERS,
            json={
                "name": catalog_name,
                "database_instance_name": instance_name,
                "database_name": db_name,
                "create_database_if_not_exists": True,
            }
        )
        if not create.ok:
            print(f"  Create catalog response: {create.text}")
        create.raise_for_status()
        print(f"  Catalog {catalog_name} created.")
    else:
        resp.raise_for_status()
        print(f"  Catalog {catalog_name} already exists.")


def get_synced_table(name: str) -> dict | None:
    resp = requests.get(f"{BASE_URL}/api/2.0/database/synced_tables/{name}", headers=HEADERS)
    if resp.status_code in (404, 400):
        return None
    resp.raise_for_status()
    return resp.json()


def create_synced_table(source: str, target: str) -> dict:
    payload = {
        "name": target,
        "spec": {
            "source_table_full_name": source,
            "primary_key_columns": ["session_id"],
            "scheduling_policy": "TRIGGERED",
        },
    }
    resp = requests.post(f"{BASE_URL}/api/2.0/database/synced_tables", headers=HEADERS, json=payload)
    if resp.status_code == 409:
        print(f"  Synced table already exists.")
        return get_synced_table(target)
    if not resp.ok:
        print(f"  Create response: {resp.text}")
    resp.raise_for_status()
    return resp.json()


def wait_for_synced_table(name: str, timeout_secs: int = 600) -> dict:
    start = time.time()
    while True:
        table = get_synced_table(name)
        if table is None:
            raise RuntimeError(f"Synced table {name} not found after creation")
        sync_status = table.get("data_synchronization_status", {})
        state = sync_status.get("detailed_state", "UNKNOWN")
        print(f"  Synced table state: {state}")
        if "ACTIVE" in state or state == "SNAPSHOT_COMPLETE":
            return table
        if "FAIL" in state:
            raise RuntimeError(f"Synced table failed: {json.dumps(sync_status, indent=2)}")
        if time.time() - start > timeout_secs:
            print(f"Note: Timed out waiting ({state}) — proceeding, sync may complete later.")
            return table
        time.sleep(30)


# --- Step 1: Ensure Lakebase instance is running ---
print("\nStep 1: Ensuring Lakebase instance is AVAILABLE...")
inst = ensure_instance_running(DB_INSTANCE)
print(f"  Instance ready at: {inst.get('read_write_dns')}")

# otto_demo already exists — skip catalog creation step

# --- Step 2: Create synced table ---
print(f"\nStep 3: Creating synced table {SYNCED_TABLE}...")
existing = get_synced_table(SYNCED_TABLE)
if existing:
    state = existing.get("data_synchronization_status", {}).get("detailed_state", "UNKNOWN")
    print(f"  Synced table already exists (state={state})")
    result = existing
else:
    result = create_synced_table(SOURCE_TABLE, SYNCED_TABLE)
    print("  Waiting for initial snapshot to complete...")
    result = wait_for_synced_table(SYNCED_TABLE)

sync_status = result.get("data_synchronization_status", {})
print(f"\n✓ Lakebase Online Feature Store ready!")
print(f"  Synced table: {SYNCED_TABLE}")
print(f"  Lakebase instance: {DB_INSTANCE} ({inst.get('read_write_dns')})")
print(f"  Sync state: {sync_status.get('detailed_state', 'N/A')}")
print(f"  Primary key: session_id")
print(f"  Source: {SOURCE_TABLE}")
print(f"\nUse this table for real-time feature lookup in bot detection inference!")

"""
SecondDinner Serving Core — Step 5: Lakebase Online Feature Store

Creates a Lakebase synced table from the cards Delta table for
sub-millisecond card feature lookup.

Output: otto_demo.snap_synergy.cards_online (Lakebase synced table)
"""
import os
import time
import json
import requests

SOURCE_TABLE = "otto_demo.snap_synergy.cards"
DB_INSTANCE = "demo-database"
SYNCED_TABLE = "otto_demo.snap_synergy.cards_online"

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
print(f"Source: {SOURCE_TABLE}")
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
            "primary_key_columns": ["card_name"],
            "scheduling_policy": "TRIGGERED",
        },
        "database_instance_name": DB_INSTANCE,
        "logical_database_name": "default",
    }
    resp = requests.post(f"{BASE_URL}/api/2.0/database/synced_tables", headers=HEADERS, json=payload)
    if resp.status_code == 409:
        print(f"  Synced table already exists.")
        return get_synced_table(target)
    if not resp.ok:
        print(f"  Create response ({resp.status_code}): {resp.text}")
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


# Step 1: Ensure Lakebase instance
print("\nStep 1: Ensuring Lakebase instance is AVAILABLE...")
inst = ensure_instance_running(DB_INSTANCE)
print(f"  Instance ready at: {inst.get('read_write_dns')}")

# Step 2: Create synced table
print(f"\nStep 2: Creating synced table {SYNCED_TABLE}...")
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
print(f"  Lakebase instance: {DB_INSTANCE}")
print(f"  Sync state: {sync_status.get('detailed_state', 'N/A')}")
print(f"  Primary key: card_name")
print(f"  Source: {SOURCE_TABLE}")

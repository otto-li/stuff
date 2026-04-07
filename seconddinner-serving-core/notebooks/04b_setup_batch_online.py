"""
SecondDinner Serving Core — Step 4b: rETL Batch Scores to Lakebase

Creates a Lakebase instance 'otto-demo.sd' and syncs the batch_synergy_scores
Delta table into it for sub-ms key-value lookups.

This is the rETL (reverse ETL) pattern: offline batch scores computed in Spark
are synced into a low-latency Postgres-compatible store for real-time serving.

Output: otto_demo.snap_synergy.batch_synergy_online (Lakebase synced table)
"""
import os
import time
import json
import requests

SOURCE_TABLE = "otto_demo.snap_synergy.batch_synergy_scores"
DB_INSTANCE = "otto-demo-sd"
SYNCED_TABLE = "otto_demo.snap_synergy.batch_synergy_online"

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
print(f"Lakebase instance: {DB_INSTANCE}")
print(f"Synced table: {SYNCED_TABLE}")


def get_instance(name: str) -> dict | None:
    resp = requests.get(f"{BASE_URL}/api/2.0/database/instances/{name}", headers=HEADERS)
    if resp.status_code in (404, 400):
        return None
    resp.raise_for_status()
    return resp.json()


def create_instance(name: str) -> dict:
    payload = {
        "name": name,
        "capacity": "CU_1",
        "autoscaling": {
            "min_capacity": "CU_1",
            "max_capacity": "CU_4",
        },
    }
    resp = requests.post(f"{BASE_URL}/api/2.0/database/instances", headers=HEADERS, json=payload)
    if resp.status_code == 409:
        print(f"  Instance already exists.")
        return get_instance(name)
    if not resp.ok:
        print(f"  Create response ({resp.status_code}): {resp.text}")
    resp.raise_for_status()
    return resp.json()


def wait_for_instance(name: str, timeout_secs: int = 600) -> dict:
    start = time.time()
    while True:
        inst = get_instance(name)
        if inst is None:
            raise RuntimeError(f"Instance {name} not found")
        state = inst.get("state", "UNKNOWN")
        print(f"  Instance state: {state}")
        if state == "AVAILABLE":
            return inst
        if state == "STOPPED":
            print(f"  Starting stopped instance...")
            requests.patch(
                f"{BASE_URL}/api/2.0/database/instances/{name}",
                headers=HEADERS, json={"stopped": False}
            )
        if state == "FAILED":
            raise RuntimeError(f"Instance failed: {state}")
        if time.time() - start > timeout_secs:
            raise TimeoutError(f"Instance not available after {timeout_secs}s")
        time.sleep(20)


def get_synced_table(name: str) -> dict | None:
    resp = requests.get(f"{BASE_URL}/api/2.0/database/synced_tables/{name}", headers=HEADERS)
    if resp.status_code in (404, 400):
        return None
    resp.raise_for_status()
    return resp.json()


def create_synced_table(source: str, target: str, pk_columns: list[str]) -> dict:
    payload = {
        "name": target,
        "spec": {
            "source_table_full_name": source,
            "primary_key_columns": pk_columns,
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
            raise RuntimeError(f"Synced table {name} not found")
        sync_status = table.get("data_synchronization_status", {})
        state = sync_status.get("detailed_state", "UNKNOWN")
        print(f"  Synced table state: {state}")
        if "ONLINE" in state or "ACTIVE" in state or state == "SNAPSHOT_COMPLETE":
            return table
        if "FAIL" in state:
            raise RuntimeError(f"Synced table failed: {json.dumps(sync_status, indent=2)}")
        if time.time() - start > timeout_secs:
            print(f"Note: Timed out waiting ({state}) — proceeding, sync may complete later.")
            return table
        time.sleep(30)


# --- Step 1: Create or ensure Lakebase instance ---
print("\n═══ Step 1: Lakebase Instance ═══")
inst = get_instance(DB_INSTANCE)
if inst is None:
    print(f"  Creating Lakebase instance '{DB_INSTANCE}'...")
    inst = create_instance(DB_INSTANCE)
    print("  Waiting for instance to be AVAILABLE...")
    inst = wait_for_instance(DB_INSTANCE)
else:
    state = inst.get("state", "UNKNOWN")
    print(f"  Instance exists (state={state})")
    if state != "AVAILABLE":
        inst = wait_for_instance(DB_INSTANCE)

print(f"  DNS: {inst.get('read_write_dns')}")

# --- Step 2: Create synced table (rETL) ---
print(f"\n═══ Step 2: rETL Synced Table ═══")
existing = get_synced_table(SYNCED_TABLE)
if existing:
    state = existing.get("data_synchronization_status", {}).get("detailed_state", "UNKNOWN")
    print(f"  Synced table already exists (state={state})")
    result = existing
else:
    print(f"  Creating synced table {SYNCED_TABLE}...")
    print(f"  Source: {SOURCE_TABLE}")
    print(f"  Primary key: (card_a, card_b)")
    result = create_synced_table(SOURCE_TABLE, SYNCED_TABLE, ["card_a", "card_b"])
    print("  Waiting for initial snapshot...")
    result = wait_for_synced_table(SYNCED_TABLE)

sync_status = result.get("data_synchronization_status", {})
print(f"\n✓ rETL complete — batch scores synced to Lakebase!")
print(f"  Synced table: {SYNCED_TABLE}")
print(f"  Lakebase instance: {DB_INSTANCE}")
print(f"  DNS: {inst.get('read_write_dns')}")
print(f"  Sync state: {sync_status.get('detailed_state', 'N/A')}")
print(f"  Primary key: (card_a, card_b)")
print(f"  Source: {SOURCE_TABLE}")
print(f"\n  rETL pattern: Delta (batch) → Lakebase (online) → App (sub-ms queries)")

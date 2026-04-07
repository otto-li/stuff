import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))


def get_workspace_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-otto-demo")
    return WorkspaceClient(profile=profile)


def get_oauth_token() -> str:
    client = get_workspace_client()
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Could not obtain OAuth token")


def get_workspace_host() -> str:
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    client = get_workspace_client()
    return client.config.host


def get_zerobus_config() -> dict:
    workspace_url = get_workspace_host()
    return {
        # ZEROBUS_HOST: dedicated gRPC endpoint, e.g.
        #   https://<shard-id>.zerobus.<region>.cloud.databricks.com
        # Falls back to workspace URL (Unimplemented → demo mode fallback in producer)
        "host": os.environ.get("ZEROBUS_HOST", workspace_url),
        "unity_catalog_url": workspace_url,
        "table_name": os.environ.get("ZEROBUS_TABLE_NAME", "ol.snap.game_events"),
        "warehouse_id": os.environ.get("DATABRICKS_WAREHOUSE_ID", "3baa12157046a0c0"),
    }

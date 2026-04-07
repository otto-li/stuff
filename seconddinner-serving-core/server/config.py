import os
from databricks.sdk import WorkspaceClient
from openai import AsyncOpenAI

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

CATALOG = "otto_demo"
SCHEMA = "snap_synergy"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "3baa12157046a0c0")
SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4-6")
CLUSTER_ENDPOINT = "snap-card-clusterer"


def get_workspace_client() -> WorkspaceClient:
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-otto-demo")
    return WorkspaceClient(profile=profile)


def get_oauth_token() -> str:
    w = get_workspace_client()
    if w.config.token:
        return w.config.token
    auth_headers = w.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Unable to obtain OAuth token")


def get_workspace_host() -> str:
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    w = get_workspace_client()
    return w.config.host


def get_serving_endpoint_url(endpoint_name: str) -> str:
    host = get_workspace_host()
    return f"{host}/serving-endpoints/{endpoint_name}/invocations"


def get_llm_client() -> AsyncOpenAI:
    host = get_workspace_host()
    if IS_DATABRICKS_APP:
        token = os.environ.get("DATABRICKS_TOKEN") or get_oauth_token()
    else:
        token = get_oauth_token()
    return AsyncOpenAI(api_key=token, base_url=f"{host}/serving-endpoints")


def get_sql_url() -> str:
    host = get_workspace_host()
    return f"{host}/api/2.0/sql/statements"

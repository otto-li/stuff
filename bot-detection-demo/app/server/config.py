import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))


def get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient configured for the current environment."""
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-otto-demo")
    return WorkspaceClient(profile=profile)


def get_oauth_token() -> str:
    """Get an OAuth bearer token for API calls."""
    w = get_workspace_client()
    if w.config.token:
        return w.config.token
    auth_headers = w.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Unable to obtain OAuth token")


def get_workspace_host() -> str:
    """Return the workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    w = get_workspace_client()
    return w.config.host


def get_serving_endpoint_url(endpoint_name: str = "bot-detector-endpoint") -> str:
    """Build the full URL for a model serving endpoint."""
    host = get_workspace_host()
    return f"{host}/serving-endpoints/{endpoint_name}/invocations"

"""Deploy the Pokemon TCG Live supervisor agent to Databricks."""

import os
import mlflow
from databricks.sdk import WorkspaceClient

from supervisor import PokemonTCGSupervisor

ENDPOINT_NAME = "pokemon-tcg-live-mas"
CATALOG = "pokemon_tcg_live"
SCHEMA = "gold"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.pokemon_tcg_supervisor"


def main():
    # Set up MLflow to use Unity Catalog
    mlflow.set_registry_uri("databricks-uc")

    w = WorkspaceClient()

    print("Logging model to MLflow...")
    with mlflow.start_run(run_name="pokemon-tcg-supervisor"):
        model_info = mlflow.pyfunc.log_model(
            artifact_path="supervisor",
            python_model=PokemonTCGSupervisor(),
            registered_model_name=MODEL_NAME,
            pip_requirements=[
                "databricks-sdk>=0.30.0",
                "openai>=1.0.0",
                "mlflow>=2.10.0",
            ],
            input_example={"messages": [{"role": "user", "content": "How many active players are in NA?"}]},
        )
        print(f"Model logged: {model_info.model_uri}")

    # Get latest version
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    latest = max(versions, key=lambda v: int(v.version))
    print(f"Latest version: {latest.version}")

    # Create or update serving endpoint
    print(f"Deploying serving endpoint: {ENDPOINT_NAME}...")
    try:
        endpoint = w.serving_endpoints.create(
            name=ENDPOINT_NAME,
            config={
                "served_entities": [
                    {
                        "entity_name": MODEL_NAME,
                        "entity_version": str(latest.version),
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True,
                        "environment_vars": {
                            "LLM_ENDPOINT": "databricks-claude-sonnet-4",
                        },
                    }
                ]
            },
        )
        print(f"Endpoint created! ID: {endpoint.id}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("Endpoint exists, updating config...")
            w.serving_endpoints.update_config(
                name=ENDPOINT_NAME,
                served_entities=[
                    {
                        "entity_name": MODEL_NAME,
                        "entity_version": str(latest.version),
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True,
                        "environment_vars": {
                            "LLM_ENDPOINT": "databricks-claude-sonnet-4",
                        },
                    }
                ],
            )
            print("Endpoint updated!")
        else:
            raise

    print(f"\nDone! Endpoint: {ENDPOINT_NAME}")
    print("Wait for it to become READY, then point the chat UI and Slack bot at it.")


if __name__ == "__main__":
    main()

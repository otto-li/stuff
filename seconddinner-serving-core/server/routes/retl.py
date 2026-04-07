"""Panel 6: rETL / Lakebase — query batch synergy scores via direct Postgres connection."""

import time
import psycopg
import httpx
from fastapi import APIRouter, HTTPException

from ..config import get_oauth_token, get_workspace_client, get_sql_url, CATALOG, SCHEMA, WAREHOUSE_ID

router = APIRouter()

PG_HOST = "ep-sweet-mud-d2q4etd3.database.us-east-1.cloud.databricks.com"
PG_DB = "databricks_postgres"


def get_lakebase_connection():
    """Get a Postgres connection to Lakebase using the workspace client's auth."""
    w = get_workspace_client()
    # Try M2M OAuth token from the SDK
    auth_headers = w.config.authenticate()
    token = None
    if auth_headers and "Authorization" in auth_headers:
        token = auth_headers["Authorization"].replace("Bearer ", "")
    elif w.config.token:
        token = w.config.token

    if not token:
        raise RuntimeError("Unable to obtain auth token for Lakebase")

    # Try with SP application ID as user first, then 'token'
    for user in (w.config.client_id or "token", "token"):
        try:
            conn = psycopg.connect(
                host=PG_HOST,
                dbname=PG_DB,
                user=user,
                password=token,
                sslmode="require",
                autocommit=True,
                connect_timeout=10,
            )
            return conn
        except Exception:
            continue

    raise RuntimeError("All Lakebase auth methods failed")


@router.get("/retl/{card_name}")
async def retl_synergy(card_name: str, limit: int = 10):
    """Query batch synergy scores from Lakebase Postgres, SQL Warehouse fallback."""
    start = time.time()
    source = "lakebase"
    results = None

    try:
        conn = get_lakebase_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT card_b, synergy_score, shared_tags, same_cluster
                    FROM "{CATALOG}"."{SCHEMA}"."batch_synergy_online"
                    WHERE card_a = %s
                    ORDER BY synergy_score DESC
                    LIMIT %s""",
                (card_name, limit),
            )
            columns = [desc.name for desc in cur.description]
            rows = cur.fetchall()
        conn.close()
        results = [dict(zip(columns, row)) for row in rows]
    except Exception:
        # Fallback to SQL Warehouse
        source = "sql_warehouse"
        token = get_oauth_token()
        query = f"""
            SELECT card_b, synergy_score, shared_tags, same_cluster
            FROM {CATALOG}.{SCHEMA}.batch_synergy_scores
            WHERE card_a = :card_name
            ORDER BY synergy_score DESC
            LIMIT :limit
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                get_sql_url(),
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "warehouse_id": WAREHOUSE_ID,
                    "statement": query,
                    "parameters": [
                        {"name": "card_name", "value": card_name, "type": "STRING"},
                        {"name": "limit", "value": str(limit), "type": "INT"},
                    ],
                    "wait_timeout": "30s",
                    "disposition": "INLINE",
                },
            )

        if resp.status_code != 200:
            raise HTTPException(502, f"SQL warehouse error: {resp.text}")

        data = resp.json()
        if data.get("status", {}).get("state") != "SUCCEEDED":
            raise HTTPException(502, f"Query state: {data.get('status', {}).get('state')}")

        columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = data.get("result", {}).get("data_array", [])
        results = [dict(zip(columns, row)) for row in rows]

    latency_ms = round((time.time() - start) * 1000)

    return {
        "serving_type": "retl",
        "card": card_name,
        "synergy_cards": results,
        "source": source,
        "latency_ms": latency_ms,
    }

"""Panel 3: Feature Serving (Online Tables / Lakebase) — sub-ms card feature lookup via Postgres wire protocol."""

import time
import psycopg
from fastapi import APIRouter, HTTPException

from ..config import get_oauth_token, CATALOG, SCHEMA

router = APIRouter()

LAKEBASE_HOST = "instance-4089ba14-458c-4aa2-9f80-b9d9d7d7346a.database.cloud.databricks.com"
LAKEBASE_DB = "default"
TABLE = f"{CATALOG}.{SCHEMA}.cards_online"


def query_lakebase(card_name: str) -> tuple[list[str], tuple] | None:
    """Try connecting to Lakebase via Postgres wire protocol."""
    token = get_oauth_token()

    # Try port 443 first (external), then 5432 (internal)
    for port in (443, 5432):
        try:
            conn = psycopg.connect(
                host=LAKEBASE_HOST,
                port=port,
                dbname=LAKEBASE_DB,
                user="token",
                password=token,
                sslmode="require",
                autocommit=True,
                connect_timeout=5,
            )
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT * FROM "{CATALOG}"."{SCHEMA}"."cards_online" WHERE card_name = %s LIMIT 1',
                    (card_name,),
                )
                columns = [desc.name for desc in cur.description]
                row = cur.fetchone()
            conn.close()
            if row:
                return columns, row
            return None
        except Exception:
            continue
    return None


@router.get("/features/{card_name}")
async def feature_lookup(card_name: str):
    """Look up a card's full feature vector from Lakebase online table."""
    start = time.time()
    source = "lakebase"

    result = query_lakebase(card_name)

    # Fallback to SQL Warehouse if Lakebase connection fails
    if result is None:
        import httpx
        from ..config import get_sql_url, WAREHOUSE_ID

        source = "sql_warehouse"
        token = get_oauth_token()
        query = f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.cards
            WHERE card_name = :card_name LIMIT 1
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
        if not rows:
            raise HTTPException(404, f"Card '{card_name}' not found")

        features = dict(zip(columns, rows[0]))
        latency_ms = round((time.time() - start) * 1000)

        return {
            "serving_type": "feature_serving",
            "card": card_name,
            "features": features,
            "source": source,
            "latency_ms": latency_ms,
        }

    columns, row = result
    features = dict(zip(columns, row))
    latency_ms = round((time.time() - start) * 1000)

    return {
        "serving_type": "feature_serving",
        "card": card_name,
        "features": features,
        "source": source,
        "latency_ms": latency_ms,
    }

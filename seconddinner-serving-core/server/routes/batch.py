"""Panel 1: Batch Inference (rETL) — query synergy scores from Lakebase online table."""

import time
import psycopg
import httpx
from fastapi import APIRouter, HTTPException

from ..config import (
    get_oauth_token,
    get_sql_url,
    CATALOG,
    SCHEMA,
    WAREHOUSE_ID,
)

router = APIRouter()

LAKEBASE_HOST = "ep-sweet-mud-d2q4etd3.database.us-east-1.cloud.databricks.com"
LAKEBASE_DB = "default"


def query_lakebase(card_name: str, limit: int) -> list[dict] | None:
    """Try Lakebase Postgres for sub-ms batch score lookup."""
    token = get_oauth_token()
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
            return [dict(zip(columns, row)) for row in rows]
        except Exception:
            continue
    return None


@router.get("/batch/{card_name}")
async def batch_synergy(card_name: str, limit: int = 10):
    """Look up pre-computed batch synergy scores — Lakebase first, SQL Warehouse fallback."""
    start = time.time()
    source = "lakebase"

    # Try Lakebase (sub-ms)
    results = query_lakebase(card_name, limit)

    # Fallback to SQL Warehouse
    if results is None:
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
        status = data.get("status", {}).get("state")
        if status == "FAILED":
            raise HTTPException(502, f"Query failed: {data.get('status', {}).get('error', {}).get('message')}")
        if status != "SUCCEEDED":
            raise HTTPException(504, f"Query did not complete in time (state: {status})")

        columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = data.get("result", {}).get("data_array", [])
        results = [dict(zip(columns, row)) for row in rows]

    latency_ms = round((time.time() - start) * 1000)

    return {
        "serving_type": "batch",
        "card": card_name,
        "synergy_cards": results,
        "source": source,
        "latency_ms": latency_ms,
    }

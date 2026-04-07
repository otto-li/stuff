"""Panel 4: Vector Search (Semantic Similarity) — find cards with similar abilities."""

import time
import httpx
from fastapi import APIRouter, HTTPException

from ..config import get_oauth_token, get_workspace_host, get_llm_client, CATALOG, SCHEMA

router = APIRouter()

VS_INDEX = f"{CATALOG}.{SCHEMA}.card_embeddings_index"
EMBEDDING_ENDPOINT = "databricks-bge-large-en"


async def get_embedding(text: str) -> list[float]:
    """Embed text via Foundation Model API (OpenAI-compatible)."""
    client = get_llm_client()
    resp = await client.embeddings.create(input=[text], model=EMBEDDING_ENDPOINT)
    return resp.data[0].embedding


@router.get("/similar/{card_name}")
async def similar_cards(card_name: str, num_results: int = 10):
    """Query Vector Search index for semantically similar cards."""
    start = time.time()
    token = get_oauth_token()
    host = get_workspace_host()

    # First, get the card's ability text to embed
    sql_url = f"{host}/api/2.0/sql/statements"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            sql_url,
            headers={"Authorization": f"Bearer {token}"},
            json={
                "warehouse_id": "3baa12157046a0c0",
                "statement": f"SELECT ability_text FROM {CATALOG}.{SCHEMA}.cards WHERE card_name = :name LIMIT 1",
                "parameters": [{"name": "name", "value": card_name, "type": "STRING"}],
                "wait_timeout": "30s",
                "disposition": "INLINE",
            },
        )

    if resp.status_code != 200:
        raise HTTPException(502, f"SQL error: {resp.text}")

    data = resp.json()
    rows = data.get("result", {}).get("data_array", [])
    ability_text = rows[0][0] if rows else card_name

    # Embed the ability text
    query_vector = await get_embedding(ability_text)

    # Query Vector Search with the embedding vector
    url = f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json={
                "query_vector": query_vector,
                "columns": ["card_name", "ability_text"],
                "num_results": num_results + 1,
            },
        )

    if resp.status_code != 200:
        raise HTTPException(502, f"Vector Search error: {resp.text}")

    data = resp.json()
    columns = data.get("manifest", {}).get("columns", [])
    col_names = [c["name"] for c in columns]
    rows = data.get("result", {}).get("data_array", [])

    results = []
    for row in rows:
        entry = dict(zip(col_names, row))
        if entry.get("card_name", "").lower() != card_name.lower():
            results.append(entry)

    results = results[:num_results]
    latency_ms = round((time.time() - start) * 1000)

    return {
        "serving_type": "vector_search",
        "card": card_name,
        "similar_cards": results,
        "latency_ms": latency_ms,
    }

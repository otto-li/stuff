"""Panel 2: Model Serving (Real-time K-means) — call the serving endpoint."""

import time
import httpx
from fastapi import APIRouter, HTTPException

from ..config import get_oauth_token, get_serving_endpoint_url, CLUSTER_ENDPOINT

router = APIRouter()

# Retry once on timeout (cold start)
MAX_RETRIES = 2
TIMEOUT_SECS = 180.0


@router.get("/cluster/{card_name}")
async def cluster_card(card_name: str):
    """Send card name to K-means serving endpoint, get cluster + centroids."""
    start = time.time()
    token = get_oauth_token()
    url = get_serving_endpoint_url(CLUSTER_ENDPOINT)

    payload = {"dataframe_records": [{"card_name": card_name}]}

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT_SECS) as client:
                resp = await client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )

            if resp.status_code == 200:
                result = resp.json()
                latency_ms = round((time.time() - start) * 1000)
                return {
                    "serving_type": "model_serving",
                    "card": card_name,
                    "predictions": result.get("predictions", result),
                    "latency_ms": latency_ms,
                }

            if resp.status_code == 408 and attempt < MAX_RETRIES - 1:
                continue  # retry on timeout (cold start)

            raise HTTPException(resp.status_code, f"Serving endpoint error: {resp.text}")

        except httpx.ReadTimeout:
            if attempt < MAX_RETRIES - 1:
                continue
            raise HTTPException(504, "Serving endpoint timed out (cold start?)")

    raise HTTPException(504, "Serving endpoint unavailable after retries")

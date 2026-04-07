"""Panel 5: Foundation Model API (AI Gateway) — LLM-generated synergy explanation."""

import time
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ..config import get_llm_client, SERVING_ENDPOINT

router = APIRouter()

SYSTEM_PROMPT = """You are a Marvel Snap deck-building expert. When given a card name,
explain which other cards synergize well with it and why, in 2-3 concise sentences.
Focus on gameplay mechanics (On Reveal, Ongoing, Destroy, Move, Discard, etc.)
and specific card combos. Be enthusiastic but brief."""


@router.get("/explain/{card_name}")
async def explain_synergy(card_name: str, stream: bool = False):
    """Use Foundation Model to generate a synergy explanation."""
    start = time.time()
    client = get_llm_client()

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"What cards synergize well with {card_name} in Marvel Snap?"},
    ]

    if stream:
        async def event_stream():
            response = await client.chat.completions.create(
                model=SERVING_ENDPOINT,
                messages=messages,
                max_tokens=300,
                temperature=0.7,
                stream=True,
            )
            async for chunk in response:
                delta = chunk.choices[0].delta
                if delta.content:
                    yield f"data: {delta.content}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    response = await client.chat.completions.create(
        model=SERVING_ENDPOINT,
        messages=messages,
        max_tokens=300,
        temperature=0.7,
    )

    explanation = response.choices[0].message.content
    latency_ms = round((time.time() - start) * 1000)

    return {
        "serving_type": "foundation_model",
        "card": card_name,
        "explanation": explanation,
        "latency_ms": latency_ms,
    }

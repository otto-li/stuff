import os
import base64
from openai import AsyncOpenAI
from .config import get_oauth_token, get_workspace_host, IS_DATABRICKS_APP

SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")


def get_llm_client() -> AsyncOpenAI:
    host = get_workspace_host()
    if IS_DATABRICKS_APP:
        token = os.environ.get("DATABRICKS_TOKEN") or get_oauth_token()
    else:
        token = get_oauth_token()
    return AsyncOpenAI(api_key=token, base_url=f"{host}/serving-endpoints")


async def identify_pokemon_from_image(image_bytes: bytes, content_type: str) -> str:
    """Use Claude Vision to identify a Pokémon from an uploaded image."""
    client = get_llm_client()
    b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

    response = await client.chat.completions.create(
        model=SERVING_ENDPOINT,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{content_type};base64,{b64}"},
                    },
                    {
                        "type": "text",
                        "text": (
                            "You are a Pokémon expert. Look at this image and identify the Pokémon. "
                            "Respond with ONLY the exact Pokémon name in lowercase English "
                            "(e.g. 'pikachu', 'charizard', 'mewtwo'). "
                            "If the image does not contain a Pokémon, respond with 'unknown'. "
                            "Do not include any other text."
                        ),
                    },
                ],
            }
        ],
        max_tokens=50,
        temperature=0.0,
    )
    name = response.choices[0].message.content.strip().lower()
    # Sanitise — only allow alphanumeric + hyphens (valid PokéAPI slugs)
    import re
    name = re.sub(r"[^a-z0-9\-]", "", name)
    return name or "unknown"

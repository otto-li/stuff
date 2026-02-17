"""Foundation Model API for predictions."""
import os
from openai import AsyncOpenAI
from .config import get_oauth_token, get_workspace_host, IS_DATABRICKS_APP

def get_llm_client() -> AsyncOpenAI:
    """Get OpenAI-compatible client for Databricks Foundation Models."""
    host = get_workspace_host()

    if IS_DATABRICKS_APP:
        # Remote: Use service principal token
        token = os.environ.get("DATABRICKS_TOKEN") or get_oauth_token()
    else:
        # Local: Use profile token
        token = get_oauth_token()

    return AsyncOpenAI(
        api_key=token,
        base_url=f"{host}/serving-endpoints"
    )

async def predict_impressions(segment_data: dict, historical_data: list) -> list:
    """Predict next month's impressions using Foundation Model."""
    client = get_llm_client()
    
    # Prepare prompt with segment info and historical data
    prompt = f"""Given an advertiser segment with the following criteria:
- Age Bands: {segment_data.get('age_bands', [])}
- Demographics: {segment_data.get('demographics', [])}
- Locations: {segment_data.get('locations', [])}
- Interests: {segment_data.get('interests', [])}

And historical data showing impressions by day for the past 30 days:
{historical_data}

Predict the daily impressions for the next 30 days. Consider trends, seasonality, and growth patterns.
Return ONLY a JSON array of 30 numbers representing predicted daily impressions, nothing else."""
    
    try:
        response = await client.chat.completions.create(
            model="databricks-claude-sonnet-4-5",
            messages=[{
                "role": "user",
                "content": prompt
            }],
            max_tokens=1000,
            temperature=0.3,
        )
        
        # Parse response
        import json
        predictions_text = response.choices[0].message.content
        # Extract JSON array from response
        start = predictions_text.find('[')
        end = predictions_text.rfind(']') + 1
        if start >= 0 and end > start:
            predictions = json.loads(predictions_text[start:end])
            return predictions
    except Exception as e:
        print(f"LLM prediction error: {e}")
    
    # Fallback: simple trend-based prediction
    if historical_data:
        avg = sum(historical_data) / len(historical_data)
        growth_rate = 1.05  # 5% growth
        return [int(avg * growth_rate * (1 + i * 0.01)) for i in range(30)]
    
    return [1000 + i * 50 for i in range(30)]  # Default

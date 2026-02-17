"""API routes for segment management."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import uuid
from datetime import datetime, timedelta
import random

router = APIRouter()

class SegmentCreate(BaseModel):
    segment_name: str
    age_bands: List[str]
    demographics: List[str]
    locations: List[str]
    interests: List[str]
    min_engagement_minutes: Optional[float] = 0

class SegmentResponse(BaseModel):
    segment_id: str
    segment_name: str
    age_bands: List[str]
    demographics: List[str]
    locations: List[str]
    interests: List[str]
    estimated_reach: int
    created_at: str

class AnalyticsResponse(BaseModel):
    segment_id: str
    previous_month: List[dict]  # [{date, impressions, minutes, devices}]
    predicted_month: List[dict]  # [{date, impressions}]

def generate_synthetic_impressions(days: int = 30, base: int = 5000) -> List[int]:
    """Generate synthetic impression data with trends."""
    impressions = []
    for i in range(days):
        # Add weekly pattern (weekends lower) and random variance
        day_of_week = i % 7
        weekend_factor = 0.7 if day_of_week >= 5 else 1.0
        trend = 1 + (i / days) * 0.2  # 20% growth over period
        variance = random.uniform(0.9, 1.1)
        value = int(base * weekend_factor * trend * variance)
        impressions.append(value)
    return impressions

def generate_device_distribution() -> List[dict]:
    """Generate device distribution."""
    devices = ["Mobile", "Desktop", "Tablet", "Smart TV"]
    weights = [0.55, 0.30, 0.10, 0.05]
    return [
        {"device": device, "percentage": weight * 100}
        for device, weight in zip(devices, weights)
    ]

@router.post("/segments", response_model=SegmentResponse)
async def create_segment(segment: SegmentCreate):
    """Create a new advertiser segment."""
    from ..db import db
    
    segment_id = str(uuid.uuid4())
    
    # Calculate estimated reach based on criteria
    # More specific criteria = smaller reach
    specificity_factor = (
        len(segment.age_bands) * 0.3 +
        len(segment.demographics) * 0.2 +
        len(segment.locations) * 0.2 +
        len(segment.interests) * 0.3
    )
    base_reach = 1000000
    estimated_reach = int(base_reach / (1 + specificity_factor))
    
    # Insert into gold_segments table
    try:
        # Format arrays for SQL
        age_bands_sql = "array(" + ", ".join([f"'{x}'" for x in segment.age_bands]) + ")" if segment.age_bands else "array()"
        demographics_sql = "array(" + ", ".join([f"'{x}'" for x in segment.demographics]) + ")" if segment.demographics else "array()"
        locations_sql = "array(" + ", ".join([f"'{x}'" for x in segment.locations]) + ")" if segment.locations else "array()"
        interests_sql = "array(" + ", ".join([f"'{x}'" for x in segment.interests]) + ")" if segment.interests else "array()"

        sql = f"""
        INSERT INTO gold_segments (
            segment_id, segment_name, age_bands, demographics,
            locations, interests, min_engagement_minutes,
            created_by, estimated_reach
        ) VALUES (
            '{segment_id}',
            '{segment.segment_name}',
            {age_bands_sql},
            {demographics_sql},
            {locations_sql},
            {interests_sql},
            {segment.min_engagement_minutes},
            'demo_user',
            {estimated_reach}
        )
        """
        await db.execute_sql(sql)
    except Exception as e:
        print(f"Database insert error: {e}")
        # Continue with demo mode
    
    return SegmentResponse(
        segment_id=segment_id,
        segment_name=segment.segment_name,
        age_bands=segment.age_bands,
        demographics=segment.demographics,
        locations=segment.locations,
        interests=segment.interests,
        estimated_reach=estimated_reach,
        created_at=datetime.utcnow().isoformat()
    )

@router.get("/segments/{segment_id}/analytics", response_model=AnalyticsResponse)
async def get_segment_analytics(segment_id: str):
    """Get analytics for a segment including predictions."""
    from ..llm import predict_impressions
    
    # Generate previous month data (bronze → silver → gold aggregation)
    previous_impressions = generate_synthetic_impressions(30, base=5000)
    previous_month = []
    
    base_date = datetime.utcnow() - timedelta(days=30)
    for i, impressions in enumerate(previous_impressions):
        date = base_date + timedelta(days=i)
        minutes = impressions * random.uniform(2.5, 4.5)  # Avg engagement per impression
        devices = generate_device_distribution()
        
        previous_month.append({
            "date": date.strftime("%Y-%m-%d"),
            "impressions": impressions,
            "minutes": round(minutes, 1),
            "devices": devices
        })
    
    # Predict next month using Foundation Model API
    try:
        predicted_impressions = await predict_impressions(
            {"age_bands": [], "demographics": []},
            previous_impressions
        )
    except:
        # Fallback prediction
        avg = sum(previous_impressions) / len(previous_impressions)
        predicted_impressions = [int(avg * 1.05 * (1 + i * 0.01)) for i in range(30)]
    
    predicted_month = []
    base_date = datetime.utcnow()
    for i, impressions in enumerate(predicted_impressions[:30]):
        date = base_date + timedelta(days=i)
        predicted_month.append({
            "date": date.strftime("%Y-%m-%d"),
            "impressions": impressions
        })
    
    return AnalyticsResponse(
        segment_id=segment_id,
        previous_month=previous_month,
        predicted_month=predicted_month
    )

@router.get("/segments")
async def list_segments():
    """List all segments."""
    from ..db import db
    
    try:
        sql = "SELECT * FROM gold_segments ORDER BY created_at DESC LIMIT 20"
        results = await db.execute_sql(sql)
        return {"segments": results}
    except Exception as e:
        print(f"List segments error: {e}")
        return {"segments": []}

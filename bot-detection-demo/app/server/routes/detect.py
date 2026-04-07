import re
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from server.config import get_oauth_token, get_serving_endpoint_url

router = APIRouter()


class DetectRequest(BaseModel):
    session_duration_secs: float
    num_requests: int
    avg_time_between_requests_ms: float
    page_views: int
    click_count: int
    mouse_events: int
    form_submissions: int
    js_errors: int
    user_agent: str
    referrer: Optional[str] = ""


def _ua_risk_score(ua: str) -> float:
    ua_lower = ua.lower()
    if any(k in ua_lower for k in ["python-urllib", "scrapy"]):
        return 1.0
    if any(k in ua_lower for k in ["curl", "go-http", "java/"]):
        return 0.8
    if any(k in ua_lower for k in ["chrome", "firefox", "safari", "edge"]):
        return 0.1
    return 0.5


def _device_risk_score(ua: str) -> float:
    ua_lower = ua.lower()
    if any(k in ua_lower for k in ["server", "unknown", "bot", "spider", "crawl"]):
        return 0.9
    if any(k in ua_lower for k in ["mobile", "android", "iphone", "ipad"]):
        return 0.15
    return 0.1


def _referrer_risk(referrer: Optional[str]) -> float:
    if not referrer or referrer.lower() in ("", "none", "programmatic"):
        return 0.6
    return 0.2


def compute_features(req: DetectRequest) -> dict:
    """Derive all 21 model features from the raw behavioral signals."""
    duration = max(req.session_duration_secs, 0.001)
    page_views = max(req.page_views, 1)
    num_requests = max(req.num_requests, 1)

    requests_per_minute = req.num_requests / (duration / 60.0)
    clicks_per_page = req.click_count / page_views
    is_high_speed = 1 if req.avg_time_between_requests_ms < 200 else 0
    ua_risk = _ua_risk_score(req.user_agent)
    geo_risk = 0.4  # unknown default
    device_risk = _device_risk_score(req.user_agent)
    has_mouse = 1 if req.mouse_events > 10 else 0
    mouse_per_click = (
        req.mouse_events / req.click_count if req.click_count > 0 else 0
    )
    js_score = 0.3 if req.js_errors > 0 else 0.7
    click_through_depth = req.page_views / num_requests
    missing_browser_signals = 0
    tls_risk = 0.1
    referrer_risk = _referrer_risk(req.referrer)

    heuristic = (
        (0.3 if is_high_speed else 0.0)
        + (0.4 if ua_risk >= 0.8 else 0.0)
        + (0.15 if has_mouse == 0 else 0.0)
        + (0.15 if missing_browser_signals == 1 else 0.0)
    )

    return {
        "session_duration_secs": round(duration, 4),
        "num_requests": req.num_requests,
        "avg_time_between_requests_ms": round(req.avg_time_between_requests_ms, 4),
        "page_views": req.page_views,
        "click_count": req.click_count,
        "mouse_events": req.mouse_events,
        "form_submissions": req.form_submissions,
        "js_errors": req.js_errors,
        "requests_per_minute": round(requests_per_minute, 4),
        "clicks_per_page": round(clicks_per_page, 4),
        "is_high_speed": is_high_speed,
        "ua_risk_score": ua_risk,
        "geo_risk_score": geo_risk,
        "device_risk_score": device_risk,
        "has_mouse_activity": has_mouse,
        "mouse_events_per_click": round(mouse_per_click, 4),
        "js_execution_score": js_score,
        "click_through_depth": round(click_through_depth, 4),
        "missing_browser_signals": missing_browser_signals,
        "tls_risk": tls_risk,
        "referrer_risk": referrer_risk,
        "heuristic_bot_score": round(heuristic, 4),
    }


async def _call_endpoint(url: str, token: str, payload: dict, timeout: float) -> httpx.Response:
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await client.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )


@router.post("/detect")
async def detect_bot(req: DetectRequest):
    features = compute_features(req)

    try:
        token = get_oauth_token()
        url = get_serving_endpoint_url()
        payload = {"dataframe_records": [features]}

        # Retry once on timeout — endpoint may be cold-starting (scale-to-zero)
        resp = None
        for attempt in range(2):
            try:
                resp = await _call_endpoint(url, token, payload, timeout=180.0)
                break
            except httpx.ReadTimeout:
                if attempt == 0:
                    print("Serving endpoint timed out (cold start?), retrying...")
                    continue
                raise HTTPException(
                    status_code=503,
                    detail="Serving endpoint is warming up — please retry in a moment.",
                )

        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Serving endpoint returned {resp.status_code}: {resp.text}",
            )

        data = resp.json()
        predictions = data.get("predictions", [])
        prediction = int(predictions[0]) if predictions else 0
        model_says_bot = prediction == 1

        heuristic = features["heuristic_bot_score"]

        # Combine model prediction with heuristic: if either signals
        # bot strongly, flag it.  The heuristic threshold of 0.6 catches
        # obviously scripted sessions even when the ML model is lenient.
        is_bot = model_says_bot or heuristic >= 0.6
        confidence = max(heuristic, 0.5) if is_bot else (1.0 - heuristic)

        return {
            "is_bot": is_bot,
            "confidence": round(min(confidence, 1.0), 2),
            "features": features,
        }

    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=502, detail=f"Endpoint call failed ({type(exc).__name__}): {exc or 'no details'}")
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")

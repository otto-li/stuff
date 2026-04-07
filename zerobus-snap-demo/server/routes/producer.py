import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional

from ..producer import producer_manager

router = APIRouter()
ws_router = APIRouter()


class StartRequest(BaseModel):
    rate: Optional[int] = 5


@router.get("/stats")
async def get_stats():
    return producer_manager.stats.to_dict()


@router.get("/debug")
async def get_debug():
    from ..producer import ZEROBUS_SDK_AVAILABLE, ZEROBUS_IMPORT_ERROR
    import os, platform

    return {
        "zerobus_sdk_available": ZEROBUS_SDK_AVAILABLE,
        "zerobus_import_error": ZEROBUS_IMPORT_ERROR or None,
        "zerobus_host": os.environ.get("ZEROBUS_HOST", "NOT SET"),
        "databricks_token_present": bool(os.environ.get("DATABRICKS_TOKEN")),
        "databricks_client_id_present": bool(os.environ.get("DATABRICKS_CLIENT_ID")),
        "databricks_client_secret_present": bool(os.environ.get("DATABRICKS_CLIENT_SECRET")),
        "zerobus_client_id_present": bool(os.environ.get("ZEROBUS_CLIENT_ID")),
        "zerobus_client_secret_present": bool(os.environ.get("ZEROBUS_CLIENT_SECRET")),
        "zerobus_client_id_prefix": os.environ.get("ZEROBUS_CLIENT_ID", "")[:8] or "NOT SET",
        "databricks_host": os.environ.get("DATABRICKS_HOST", "NOT SET"),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "last_error_full": producer_manager.stats.last_error,
        "table_name": os.environ.get("ZEROBUS_TABLE_NAME", "NOT SET"),
    }


@router.post("/start")
async def start_producer(req: StartRequest = StartRequest()):
    await producer_manager.start(rate=req.rate or 5)
    return {"status": "ok", "state": producer_manager.stats.state}


@router.post("/stop")
async def stop_producer():
    await producer_manager.stop()
    return {"status": "ok", "state": producer_manager.stats.state}


@router.post("/kill")
async def kill_producer():
    await producer_manager.kill()
    return {
        "status": "ok",
        "state": producer_manager.stats.state,
        "acked_at_kill": producer_manager.stats.acked_at_kill,
        "unacked_at_kill": producer_manager.stats.unacked_at_kill,
    }


@router.post("/resume")
async def resume_producer():
    await producer_manager.resume()
    return {"status": "ok", "state": producer_manager.stats.state}


@router.post("/spike")
async def throughput_spike():
    await producer_manager.spike()
    return {"status": "ok", "message": "Spiking to 500 events/sec for 5s"}


@router.post("/schema-violation")
async def send_schema_violation():
    await producer_manager.send_schema_violation()
    return {"status": "ok", "message": "Schema violation event sent"}


@ws_router.websocket("/ws/producer")
async def websocket_producer(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            stats = producer_manager.stats.to_dict()
            await websocket.send_json(stats)
            await asyncio.sleep(0.25)  # 4 fps
    except WebSocketDisconnect:
        pass
    except Exception:
        pass

from fastapi import APIRouter
from ..delta_reader import delta_reader

router = APIRouter()


@router.get("/count")
async def get_count():
    count = await delta_reader.get_event_count()
    return {"count": count}


@router.get("/breakdown")
async def get_breakdown():
    rows = await delta_reader.get_event_count_by_type()
    return {"breakdown": rows}


@router.get("/recent")
async def get_recent(limit: int = 10):
    rows = await delta_reader.get_recent_events(limit=limit)
    return {"events": rows}


@router.get("/rejections")
async def get_rejections():
    count = await delta_reader.get_rejection_count()
    return {"count": count}


@router.get("/hosts")
async def get_hosts():
    rows = await delta_reader.get_host_breakdown()
    return {"hosts": rows}

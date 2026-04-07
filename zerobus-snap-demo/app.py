import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from server.routes.producer import router as producer_router, ws_router
from server.routes.delta import router as delta_router

app = FastAPI(title="Zerobus Snap Demo", version="1.0.0")

app.include_router(producer_router, prefix="/api/producer")
app.include_router(delta_router, prefix="/api/delta")
app.include_router(ws_router)  # WebSocket at /ws/producer (no prefix)

# Serve built React frontend
frontend_dist = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_dist):
    app.mount(
        "/assets",
        StaticFiles(directory=os.path.join(frontend_dist, "assets")),
        name="assets",
    )

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(os.path.join(frontend_dist, "index.html"))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

app = FastAPI(title="SecondDinner Serving Core")

from server.routes.batch import router as batch_router
from server.routes.model_serving import router as model_serving_router
from server.routes.feature_serving import router as feature_serving_router
from server.routes.vector_search import router as vector_search_router
from server.routes.foundation_model import router as foundation_model_router
from server.routes.retl import router as retl_router

app.include_router(batch_router, prefix="/api")
app.include_router(model_serving_router, prefix="/api")
app.include_router(feature_serving_router, prefix="/api")
app.include_router(vector_search_router, prefix="/api")
app.include_router(foundation_model_router, prefix="/api")
app.include_router(retl_router, prefix="/api")

frontend_dir = os.path.join(os.path.dirname(__file__), "frontend", "dist")

if os.path.exists(frontend_dir):
    assets_dir = os.path.join(frontend_dir, "assets")
    if os.path.exists(assets_dir):
        app.mount(
            "/assets",
            StaticFiles(directory=assets_dir),
            name="assets",
        )

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(os.path.join(frontend_dir, "index.html"))

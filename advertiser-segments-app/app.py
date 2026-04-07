"""Main FastAPI application for Advertiser Segments."""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI(
    title="Advertiser Segment Builder",
    description="Create targeted advertiser segments and view analytics"
)

# CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import routes
from server.routes import segments
app.include_router(segments.router, prefix="/api", tags=["segments"])

# Initialize database tables on startup
@app.on_event("startup")
async def startup_event():
    from server.db import db
    try:
        await db.initialize_tables()
        print("✓ Unity Catalog tables initialized")
    except Exception as e:
        print(f"⚠ Database initialization error: {e}")
        print("  Running in demo mode without persistence")

# Serve static frontend files
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend", "static")

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve the main HTML page."""
    index_file = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_file):
        with open(index_file) as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Frontend not built yet</h1>")

# Mount static assets
if os.path.exists(frontend_dir):
    app.mount("/css", StaticFiles(directory=os.path.join(frontend_dir, "css")), name="css")
    app.mount("/js", StaticFiles(directory=os.path.join(frontend_dir, "js")), name="js")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "advertiser-segments"}

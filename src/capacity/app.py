"""FastAPI app for sprint capacity input."""

import logging
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pathlib import Path

from src.capacity.routes import router
from src.config import get_config

logger = logging.getLogger(__name__)

app = FastAPI(
    title="SDM Metrics - Capacity Input",
    description="Lightweight web form for sprint capacity entry",
    version="0.1.0",
)

# Include routes
app.include_router(router, prefix="/api")


@app.get("/", response_class=HTMLResponse)
def root():
    """Serve capacity input form."""
    template_path = Path("src/capacity/templates/capacity_form.html")
    if template_path.exists():
        with open(template_path) as f:
            return f.read()
    return "<h1>Capacity Input</h1><p>Coming soon...</p>"


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.on_event("startup")
async def startup():
    """Initialize on startup."""
    logger.info("Capacity app started")
    config = get_config()
    logger.info(f"Using database: {config['db']['url']}")

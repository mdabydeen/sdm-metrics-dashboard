"""FastAPI app for sprint capacity input."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pathlib import Path

from src.capacity.routes import router
from src.config import get_config

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown."""
    logger.info("Capacity app started")
    config = get_config()
    logger.info(f"Using database: {config['db']['url']}")
    yield


app = FastAPI(
    title="SDM Metrics - Capacity Input",
    description="Lightweight web form for sprint capacity entry",
    version="0.1.0",
    lifespan=lifespan,
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

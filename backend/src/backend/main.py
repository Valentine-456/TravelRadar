import logging

from fastapi import FastAPI

from backend.api import config
from backend.settings import get_settings
from backend.spa import SPAStaticFiles

logger = logging.getLogger("uvicorn.error")
settings = get_settings()

app = FastAPI(title="TravelRadar API")

app.include_router(config.router, prefix="/api", tags=["config"])

if settings.static_dir.is_dir():
    app.mount("/", SPAStaticFiles(directory=settings.static_dir, html=True), name="frontend")
else:
    logger.warning("Static dir %s not found, frontend not served", settings.static_dir)

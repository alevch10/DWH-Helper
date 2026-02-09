from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from app.config import settings, configure_logging, get_logger
from app.dwh_tables_worker.router import router as dwh_router
from app.appmetrica.router import router as appmetrica_router
from app.s3.router import router as s3_router


configure_logging(level=settings.logging.level)
logger = get_logger(__name__)

app = FastAPI(
    title=settings.title,
    description=settings.description,
    version=settings.version,
    debug=settings.debug
)


@app.on_event("startup")
async def startup_event():
    logger.info("Starting %s v%s", settings.title, settings.version)
    logger.debug("DWH Database: %s", settings.dwh.database_url)
    if settings.appmetrica.api_key:
        logger.debug("AppMetrica configured")


@app.get("/", tags=["main"])
async def root():
    return {"app": settings.title, "version": settings.version, "status": "running"}


app.include_router(dwh_router, prefix="/dwh")
app.include_router(appmetrica_router, prefix="/appmetrica")
app.include_router(s3_router, prefix="")

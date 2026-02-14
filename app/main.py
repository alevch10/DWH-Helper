from fastapi import FastAPI

from app.config.logger import configure_logging, get_logger
from app.config.settings import settings

from app.db.router import router as db_router
from app.appmetrica.router import router as appmetrica_router
from app.s3.router import router as s3_router
from app.amplitude.router import router as amplitude_router
from app.etl.router import router as etl_router
from app.db.repository import close_repository

configure_logging(level=settings.logging.level)
logger = get_logger(__name__)

app = FastAPI(
    title=settings.title,
    description=(
        f"{settings.description}\n\n"
        f"[Получить API-токен](https://oauth.yandex.ru/authorize?response_type=token&client_id={settings.yandex.client_id})"
    ),
    version=settings.version,
    debug=settings.debug,
)


@app.on_event("startup")
async def startup_event():
    logger.info("Starting %s v%s", settings.title, settings.version)


@app.get("/health", tags=["Main"])
async def root():
    return {"app": settings.title, "version": settings.version, "status": "running"}


app.include_router(amplitude_router, prefix="/amplitude", tags=["Amplitude"])
app.include_router(appmetrica_router, prefix="/appmetrica", tags=["AppMetrica"])
app.include_router(db_router, prefix="/db", tags=["DB"])
app.include_router(s3_router, prefix="/s3", tags=["S3"])
app.include_router(etl_router, prefix="/etl", tags=["ETL"])


@app.on_event("shutdown")
async def shutdown_event():
    close_repository()
    logger.info("Application shutdown, DB connection pool closed")

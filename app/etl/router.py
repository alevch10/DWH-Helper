import yaml
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from app.auth.deps import require_write, User
from app.etl.transformer import process_universal_etl, ProcessingInterrupted
from app.etl.config_models import ETLConfig
from app.etl.schemas import TransformResponse
from app.config.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


def _extract_oauth_token(request: Request) -> str | None:
    """Извлекает OAuth-токен из заголовка Authorization."""
    auth = request.headers.get("Authorization")
    if not auth:
        return None
    parts = auth.split()
    if len(parts) == 2 and parts[0] in ("Bearer", "OAuth"):
        return parts[1]
    return None


@router.post(
    "/transformer",
    summary="Универсальный ETL-загрузчик",
    description="""
    ## Универсальный ETL-загрузчик данных в DWH

    Поддерживаемые источники:
    - **S3** – ZIP-архивы с NDJSON
    - **Yandex Metrica** – Logs API (потоковая загрузка)
    - **AppMetrica** – Logs API (CSV/JSON)

    Принимает YAML-конфигурацию в теле запроса.
    Content-Type: application/x-yaml

    ### Параметры возобновления (query):
    - `start_after_file` – S3-ключ файла (только для S3)
    - `start_after_line` – строка внутри файла (только для S3)
    - `start_date` – дата YYYY-MM-DD для Яндекс.Метрики и AppMetrica
    """,
    openapi_extra={
        "requestBody": {
            "content": {
                "application/x-yaml": {
                    "schema": ETLConfig.model_json_schema(),
                    "examples": {
                        "example-s3": {
                            "summary": "S3 + ZIP/NDJSON",
                            "value": {
                                "source": {
                                    "type": "s3",
                                    "bucket": "dwh-helper-data",
                                    "prefix": "amplitude_web/2022/",
                                    "sort": {
                                        "by": "key",
                                        "natural": True,
                                        "order": "asc",
                                    },
                                },
                                "tables": [
                                    {
                                        "name": "amplitude_web.events",
                                        "primary_key": ["uuid", "event_time"],
                                        "on_conflict": "DO NOTHING",
                                        "fields": [
                                            {
                                                "target": "uuid",
                                                "sources": ["uuid"],
                                                "type": "uuid",
                                                "required": True,
                                            },
                                            {
                                                "target": "event_time",
                                                "sources": ["event_time"],
                                                "type": "datetime",
                                                "required": True,
                                            },
                                        ],
                                    }
                                ],
                            },
                        },
                        "example-yandex-metrika": {
                            "summary": "Яндекс.Метрика (нормализованные таблицы)",
                            "value": {
                                "source": {
                                    "type": "yandex_metrika",
                                    "counter_id": 106613495,
                                    "date_from": "2026-02-01",
                                    "date_to": "2026-02-07",
                                    "source": "hits",
                                    "fields": [
                                        "ym:pv:watchID",
                                        "ym:pv:clientID",
                                        "ym:pv:dateTime",
                                        "ym:pv:URL",
                                    ],
                                    "chunk_days": 7,
                                },
                                "tables": [
                                    {
                                        "name": "yandex_metrika.events",
                                        "batch_size": 5000,
                                        "fields": [
                                            {
                                                "target": "watch_id",
                                                "sources": ["watchID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "client_id",
                                                "sources": ["clientID"],
                                                "type": "integer",
                                            },
                                        ],
                                    }
                                ],
                            },
                        },
                        "example-appmetrica": {
                            "summary": "AppMetrica (CSV/JSON)",
                            "value": {
                                "source": {
                                    "type": "appmetrica",
                                    "application_id": 473434,
                                    "date_since": "2026-05-01",
                                    "date_until": "2026-05-07",
                                    "export_format": "csv",
                                    "fields": [
                                        "event_datetime",
                                        "event_json",
                                        "profile_id",
                                        "event_name",
                                    ],
                                    "chunk_days": 7,
                                },
                                "tables": [
                                    {
                                        "name": "appmetrica.events",
                                        "batch_size": 5000,
                                        "fields": [
                                            {
                                                "target": "event_time",
                                                "sources": ["event_datetime"],
                                                "type": "datetime",
                                            },
                                            {
                                                "target": "event_data",
                                                "sources": ["event_json"],
                                                "type": "json",
                                            },
                                            {
                                                "target": "profile_id",
                                                "sources": ["profile_id"],
                                                "type": "string",
                                            },
                                        ],
                                    }
                                ],
                            },
                        },
                    },
                }
            },
            "required": True,
        },
    },
)
async def etl_transformer(
    request: Request,
    start_after_file: str = Query(None, description="S3: S3-ключ файла"),
    start_after_line: int = Query(0, description="S3: номер строки (0‑based)"),
    start_date: str = Query(
        None, description="Яндекс.Метрика / AppMetrica: дата возобновления YYYY-MM-DD"
    ),
    user: User = Depends(require_write),
):
    # 1. Проверка Content-Type
    if "application/x-yaml" not in request.headers.get("Content-Type", ""):
        raise HTTPException(415, "Content-Type must be application/x-yaml")

    # 2. Чтение и парсинг YAML
    try:
        body_bytes = await request.body()
        config_yaml = body_bytes.decode("utf-8")
        config_dict = yaml.safe_load(config_yaml)
        etl_config = ETLConfig.model_validate(config_dict)
    except yaml.YAMLError as e:
        raise HTTPException(422, f"Invalid YAML: {str(e)}")
    except Exception as e:
        raise HTTPException(422, f"Config validation error: {str(e)}")

    # 3. Извлечение OAuth-токена (для источников, требующих внешнего API)
    token = _extract_oauth_token(request)
    if etl_config.source.type in ("yandex_metrika", "appmetrica") and not token:
        raise HTTPException(
            status_code=401,
            detail=f"OAuth token is required for {etl_config.source.type} source. "
            "Provide Authorization header (Bearer <token> or OAuth <token>).",
        )

    # 4. Параметры возобновления
    source_params = {}
    if start_after_file:
        source_params["start_after_file"] = start_after_file
    if start_after_line:
        source_params["start_after_line"] = start_after_line
    if start_date:
        source_params["start_date"] = start_date

    # 5. Запуск ETL
    try:
        stats = await process_universal_etl(
            etl_config=etl_config,
            source_params=source_params or None,
            token=token,
        )
        return TransformResponse(
            status="success",
            message="ETL completed",
            statistics=dict(stats["batches"]),
        )
    except ProcessingInterrupted as e:
        logger.warning(f"ETL interrupted: {e.message}")
        return TransformResponse(
            status="interrupted",
            message=e.message,
            last_successful_file=e.last_successful_file,
            last_successful_line=e.last_successful_line,
            failed_file=e.failed_file,
            failed_line=e.failed_line,
            error_details=e.error_details,
            last_successful_date=e.last_successful_date,
            failed_date=e.failed_date,
        )
    except Exception as e:
        logger.exception("Unexpected ETL error")
        raise HTTPException(status_code=500, detail=str(e))

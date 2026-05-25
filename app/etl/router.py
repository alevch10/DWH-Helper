import yaml
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from app.auth.deps import require_write
from app.config.logger import get_logger
from app.etl.config_models import ETLConfig
from app.etl.transformer import process_universal_etl, ProcessingInterrupted

logger = get_logger(__name__)
router = APIRouter()


@router.post(
    "/transformer",
    summary="Универсальный ETL-загрузчик",
    description="""
    ## Универсальный ETL-загрузчик данных из S3 в DWH

    Принимает **YAML-конфигурацию** в теле запроса с заголовком `Content-Type: application/x-yaml`.

    ### Параметры возобновления (query):
    - `start_after_file` – S3-ключ файла, с которого продолжить
    - `start_after_line` – номер строки внутри файла (0‑based, по умолчанию 0)

    ### Пример запроса через curl:
    ```bash
    curl -X POST "http://localhost:8000/etl/transformer?start_after_file=data/2024_week_1.zip" \\
      -H "Authorization: Bearer <токен>" \\
      -H "Content-Type: application/x-yaml" \\
      --data-binary @config.yaml
    ```
    """,
    openapi_extra={
        "requestBody": {
            "content": {
                "application/x-yaml": {
                    "schema": ETLConfig.model_json_schema(),
                    "examples": {
                        "example-1": {
                            "summary": "Пример конфигурации для Amplitude Web",
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
                        }
                    },
                }
            },
            "required": True,
        }
    },
)
async def universal_transformer(
    request: Request,
    user=Depends(require_write),
):
    """
    Универсальный ETL-загрузчик.
    """
    # 1. Читаем тело как YAML
    try:
        raw_body = await request.body()
        if not raw_body:
            raise HTTPException(400, "Empty request body")
        config_dict = yaml.safe_load(raw_body)
        if not isinstance(config_dict, dict):
            raise ValueError("YAML root must be an object")
    except yaml.YAMLError as e:
        raise HTTPException(422, detail=f"Invalid YAML syntax: {e}")
    except Exception as e:
        raise HTTPException(400, detail=f"Failed to read request body: {e}")

    # 2. Валидируем через Pydantic
    try:
        etl_config = ETLConfig(**config_dict)
    except ValidationError as e:
        raise HTTPException(422, detail=e.errors())

    # 3. Извлекаем параметры возобновления из query
    start_after_file = request.query_params.get("start_after_file")
    start_after_line_str = request.query_params.get("start_after_line", "0")
    try:
        start_after_line = int(start_after_line_str)
        if start_after_line < 0:
            raise ValueError
    except ValueError:
        raise HTTPException(400, "start_after_line must be a non-negative integer")

    source_params = {}
    if start_after_file:
        source_params["start_after_file"] = start_after_file
    if start_after_line:
        source_params["start_after_line"] = start_after_line

    # 4. Запускаем ETL с объектом конфигурации (не строкой)
    try:
        stats = process_universal_etl(etl_config, source_params=source_params or None)
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "ETL completed",
                "statistics": stats,
            },
        )
    except ProcessingInterrupted as e:
        return JSONResponse(
            status_code=200,
            content={
                "status": "interrupted",
                "message": e.message,
                "failed_file": e.failed_file,
                "failed_line": e.failed_line,
                "last_successful_file": e.last_successful_file,
                "last_successful_line": e.last_successful_line,
                "error_details": e.error_details,
            },
        )
    except Exception as e:
        logger.exception("Unexpected error in universal ETL")
        raise HTTPException(500, detail=f"Internal server error: {str(e)}")

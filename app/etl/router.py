import yaml
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from app.auth.deps import require_write, User
from app.etl.transformer import process_universal_etl, ProcessingInterrupted
from app.etl.config_models import ETLConfig
from app.etl.schemas import TransformResponse
from app.config.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


# Вспомогательная функция для извлечения OAuth-токена
def _extract_oauth_token(request: Request) -> str | None:
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
    ## Универсальный ETL-загрузчик данных из S3 или Яндекс.Метрики в DWH

    Принимает **YAML-конфигурацию** в теле запроса с заголовком `Content-Type: application/x-yaml`.

    ### Параметры возобновления (query):
    - `start_after_file` – S3-ключ файла (только для S3)
    - `start_after_line` – номер строки внутри файла (0‑based, только для S3)
    - `start_date` – дата YYYY-MM-DD (только для Яндекс.Метрики)

    ### Пример запроса (S3):
    ```bash
    curl -X POST "http://localhost:8000/etl/transformer?start_after_file=data.zip" \\
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
                        "example-s3": {
                            "summary": "S3 + ZIP/NDJSON (Amplitude)",
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
                                    "counter_id": 111111111,
                                    "date_from": "2026-02-01",
                                    "date_to": "2026-02-07",
                                    "source": "hits",
                                    "fields": [
                                        "ym:pv:watchID",
                                        "ym:pv:pageViewID",
                                        "ym:pv:visitID",
                                        "ym:pv:clientID",
                                        "ym:pv:dateTime",
                                        "ym:pv:title",
                                        "ym:pv:goalsID",
                                        "ym:pv:URL",
                                        "ym:pv:referer",
                                        "ym:pv:UTMCampaign",
                                        "ym:pv:UTMContent",
                                        "ym:pv:UTMMedium",
                                        "ym:pv:UTMSource",
                                        "ym:pv:UTMTerm",
                                        "ym:pv:lastTrafficSource",
                                        "ym:pv:lastSearchEngineRoot",
                                        "ym:pv:lastSearchEngine",
                                        "ym:pv:lastAdvEngine",
                                        "ym:pv:lastSocialNetwork",
                                        "ym:pv:lastSocialNetworkProfile",
                                        "ym:pv:recommendationSystem",
                                        "ym:pv:messenger",
                                        "ym:pv:operatingSystem",
                                        "ym:pv:browser",
                                        "ym:pv:browserMajorVersion",
                                        "ym:pv:browserMinorVersion",
                                        "ym:pv:browserCountry",
                                        "ym:pv:browserEngine",
                                        "ym:pv:browserEngineVersion1",
                                        "ym:pv:browserEngineVersion2",
                                        "ym:pv:browserEngineVersion3",
                                        "ym:pv:browserEngineVersion4",
                                        "ym:pv:browserLanguage",
                                        "ym:pv:cookieEnabled",
                                        "ym:pv:deviceCategory",
                                        "ym:pv:javascriptEnabled",
                                        "ym:pv:mobilePhone",
                                        "ym:pv:mobilePhoneModel",
                                        "ym:pv:operatingSystemRoot",
                                        "ym:pv:physicalScreenHeight",
                                        "ym:pv:physicalScreenWidth",
                                        "ym:pv:screenColors",
                                        "ym:pv:screenFormat",
                                        "ym:pv:screenHeight",
                                        "ym:pv:screenOrientation",
                                        "ym:pv:screenOrientationName",
                                        "ym:pv:screenWidth",
                                        "ym:pv:windowClientHeight",
                                        "ym:pv:windowClientWidth",
                                        "ym:pv:ipAddress",
                                        "ym:pv:regionCity",
                                        "ym:pv:regionCountry",
                                        "ym:pv:isPageView",
                                        "ym:pv:link",
                                        "ym:pv:download",
                                        "ym:pv:notBounce",
                                        "ym:pv:artificial",
                                        "ym:pv:httpError",
                                        "ym:pv:shareService",
                                        "ym:pv:shareURL",
                                        "ym:pv:shareTitle",
                                        "ym:pv:params",
                                        "ym:pv:offlineCallTalkDuration",
                                        "ym:pv:offlineCallHoldDuration",
                                        "ym:pv:offlineCallMissed",
                                        "ym:pv:offlineCallTag",
                                        "ym:pv:offlineCallFirstTimeCaller",
                                        "ym:pv:offlineCallURL",
                                    ],
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
                                                "target": "page_view_id",
                                                "sources": ["pageViewID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "visit_id",
                                                "sources": ["visitID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "client_id",
                                                "sources": ["clientID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "date_time",
                                                "sources": ["dateTime"],
                                                "type": "datetime",
                                            },
                                            {
                                                "target": "title",
                                                "sources": ["title"],
                                                "type": "string",
                                            },
                                            {
                                                "target": "goals_id",
                                                "sources": ["goalsID"],
                                                "type": "json",
                                            },
                                            {
                                                "target": "url",
                                                "sources": ["URL"],
                                                "type": "string",
                                            },
                                            {
                                                "target": "referer",
                                                "sources": ["referer"],
                                                "type": "string",
                                            },
                                            {
                                                "target": "is_page_view",
                                                "sources": ["isPageView"],
                                                "type": "boolean",
                                            },
                                            {
                                                "target": "link",
                                                "sources": ["link"],
                                                "type": "boolean",
                                            },
                                            {
                                                "target": "download",
                                                "sources": ["download"],
                                                "type": "boolean",
                                            },
                                            {
                                                "target": "not_bounce",
                                                "sources": ["notBounce"],
                                                "type": "boolean",
                                            },
                                            {
                                                "target": "artificial",
                                                "sources": ["artificial"],
                                                "type": "boolean",
                                            },
                                            {
                                                "target": "http_error",
                                                "sources": ["httpError"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "share_service",
                                                "sources": ["shareService"],
                                                "type": "string",
                                            },
                                            {
                                                "target": "share_url",
                                                "sources": ["shareURL"],
                                                "type": "string",
                                            },
                                            {
                                                "target": "share_title",
                                                "sources": ["shareTitle"],
                                                "type": "string",
                                            },
                                        ],
                                    },
                                    {
                                        "name": "yandex_metrika.event_params",
                                        "batch_size": 5000,
                                        "fields": [
                                            {
                                                "target": "watch_id",
                                                "sources": ["watchID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "page_view_id",
                                                "sources": ["pageViewID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "visit_id",
                                                "sources": ["visitID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "client_id",
                                                "sources": ["clientID"],
                                                "type": "integer",
                                            },
                                            {
                                                "target": "date_time",
                                                "sources": ["dateTime"],
                                                "type": "datetime",
                                            },
                                            {
                                                "target": "params",
                                                "sources": ["params"],
                                                "type": "string",
                                            },
                                        ],
                                    },
                                ],
                            },
                        },
                    },
                },
            },
            "required": True,
        },
    },
)
async def etl_transformer(
    request: Request,
    start_after_file: str = Query(None, description="S3: ключ файла для возобновления"),
    start_after_line: int = Query(0, description="S3: номер строки"),
    start_date: str = Query(
        None, description="Яндекс.Метрика: дата для возобновления (YYYY-MM-DD)"
    ),
    user: User = Depends(require_write),
):
    # 1. Проверка Content-Type
    if "application/x-yaml" not in request.headers.get("Content-Type", ""):
        raise HTTPException(415, "Content-Type must be application/x-yaml")

    # 2. Чтение сырого YAML из тела
    try:
        body_bytes = await request.body()
        config_yaml = body_bytes.decode("utf-8")
        config_dict = yaml.safe_load(config_yaml)
        etl_config = ETLConfig.model_validate(config_dict)
    except yaml.YAMLError as e:
        raise HTTPException(422, f"Invalid YAML: {str(e)}")
    except Exception as e:
        raise HTTPException(422, f"Config validation error: {str(e)}")

    # 3. Токен для Яндекс.Метрики (если требуется)
    token = None
    if etl_config.source.type == "yandex_metrika":
        token = _extract_oauth_token(request)
        if not token:
            raise HTTPException(401, "OAuth token required for Yandex.Metrika source")

    # 4. Сбор параметров возобновления
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
        raise HTTPException(500, str(e))

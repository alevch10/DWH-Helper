from fastapi import Request, APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from app.auth.deps import require_read
from typing import Optional
import json
import io
import zipfile

from .client import client
from app.config.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/ping")
async def ping(user=Depends(require_read)):
    return {"module": "appmetrica", "status": "ok"}


@router.get("/export")
async def export_events(
    request: Request,
    application_id: Optional[str] = Query(
        None,
        description="AppMetrica application ID (uses config default if not provided)",
    ),
    skip_unavailable_shards: bool = Query(False, description="Skip unavailable shards"),
    date_since: Optional[str] = Query(
        None, description="Start date (format: YYYY-MM-DD HH:MM:SS)"
    ),
    date_until: Optional[str] = Query(
        None, description="End date (format: YYYY-MM-DD HH:MM:SS)"
    ),
    date_dimension: str = Query("default", description="Date dimension"),
    use_utf8_bom: bool = Query(True, description="Use UTF-8 BOM"),
    fields: Optional[str] = Query(None, description="Comma-separated field list"),
    export_format: str = Query(
        "csv",
        regex="^(csv|json)$",
        description="Export format: 'csv' (default) or 'json'",
    ),
    user=Depends(require_read),
):
    """
    Запросить экспорт событий из AppMetrica.

    - **date_since**, **date_until** — обязательны для ограничения периода.
    - **export_format** = `csv` (по умолчанию) или `json`.
    - При готовности данных возвращается ZIP‑архив с файлом
      `appmetrica_resp.csv` или `appmetrica_resp.json`.
    - Если экспорт ещё не готов — возвращается JSON со статусом `pending`.

    ### Пример структуры JSON (внутри архива при `export_format=json`):
    ```json
    {
      "data": [
        {
          "app_build_number": "1321",
          "profile_id": "132414",
          "os_name": "android",
          "os_version": "16",
          "device_manufacturer": "Samsung",
          "device_model": "Galaxy S24 Ultra",
          "device_type": "phone",
          "device_locale": "ru_RU",
          "device_ipv6": "::ffff:178.66.131.78",
          "app_version_name": "2.31.0",
          "event_name": "Banner Shown",
          "event_json": {"Title": "Акции «Скандинавии»", "From": "dashboard"},
          "connection_type": "wifi",
          "operator_name": "Beeline",
          "country_iso_code": "RU",
          "city": "",
          "appmetrica_device_id": "8751800227318799555",
          "installation_id": "6ad7ce881ad14c148482b12f442984ed",
          "session_id": "10000000178",
          "event_datetime": "2026-02-08 00:08:57"
        }
      ]
    }
    ```
    Поле `event_json` может содержать произвольный JSON‑объект.
    """
    auth_header = request.headers.get("Authorization")
    raw_token = auth_header.removeprefix("Bearer ").strip()

    try:
        default_fields = (
            "app_build_number,profile_id,os_name,os_version,device_manufacturer,device_model,device_type,"
            "device_locale,device_ipv6,app_version_name,event_name,event_json,connection_type,operator_name,"
            "country_iso_code,city,appmetrica_device_id,installation_id,session_id,event_datetime"
        )
        fields_param = fields or default_fields

        # Запрос к AppMetrica
        result = await client.fetch_export(
            application_id=application_id,
            skip_unavailable_shards=skip_unavailable_shards,
            date_since=date_since,
            date_until=date_until,
            date_dimension=date_dimension,
            use_utf8_bom=use_utf8_bom,
            fields=fields_param,
            export_format=export_format,
            api_key=raw_token,
        )

        # Если экспорт ещё не готов (pending) — возвращаем JSON с описанием
        if result.get("status") != "ready":
            return result

        # Данные готовы — упаковываем в ZIP
        data = result["result"]

        # Формируем имя файла внутри архива
        if export_format == "json":
            content = json.dumps(data, ensure_ascii=False).encode("utf-8")
            filename = "appmetrica_resp.json"
        else:  # csv
            content = data.encode("utf-8")
            filename = "appmetrica_resp.csv"

        # Создаём ZIP в памяти
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr(filename, content)
        zip_buffer.seek(0)

        # Имя скачиваемого архива
        zip_name = (
            f"appmetrica_export_{date_since or 'since'}_{date_until or 'until'}.zip"
        )

        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={zip_name}"},
        )

    except Exception as e:
        logger.exception("Export failed")
        raise HTTPException(status_code=500, detail=str(e))

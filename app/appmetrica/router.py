
from fastapi import Request, APIRouter, HTTPException, Query, Depends
from app.auth.deps import require_read
from pydantic import BaseModel, Field
from typing import Any, Optional

from .client import client

router = APIRouter(tags=["AppMetrica"])


@router.get("/ping")
async def ping(user=Depends(require_read)):
    return {"module": "appmetrica", "status": "ok"}


@router.get("/export")
async def export_events(
    request: Request,
    application_id: Optional[str] = Query(None, description="AppMetrica application ID (uses config default if not provided)"),
    skip_unavailable_shards: bool = Query(False, description="Skip unavailable shards"),
    date_since: Optional[str] = Query(None, description="Start date (format: YYYY-MM-DD HH:MM:SS)"),
    date_until: Optional[str] = Query(None, description="End date (format: YYYY-MM-DD HH:MM:SS)"),
    date_dimension: str = Query("default", description="Date dimension"),
    use_utf8_bom: bool = Query(True, description="Use UTF-8 BOM"),
    fields: Optional[str] = Query(None, description="Comma-separated field list"),
    user=Depends(require_read)
):
    """Request AppMetrica export. Date format: `YYYY-MM-DD HH:MM:SS`.

    - `application_id`: default from config if not provided
    - `skip_unavailable_shards`: default `false`
    - `date_since` and `date_until`: required to bound export
    - `fields`: comma-separated list; if omitted all standard fields are used
    """
    auth_header = request.headers.get("Authorization")
    raw_token = auth_header.removeprefix("Bearer ").strip()

    try:
        # provide default fields list if not set
        default_fields = (
            "app_build_number,profile_id,os_name,os_version,device_manufacturer,device_model,device_type,"
            "device_locale,device_ipv6,app_version_name,event_name,event_json,connection_type,operator_name,"
            "country_iso_code,city,appmetrica_device_id,installation_id,session_id,event_datetime"
        )
        fields_param = fields or default_fields
        result = await client.fetch_export(
            application_id=application_id,
            skip_unavailable_shards=skip_unavailable_shards,
            date_since=date_since,
            date_until=date_until,
            date_dimension=date_dimension,
            use_utf8_bom=use_utf8_bom,
            fields=fields_param,
            api_key=raw_token,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

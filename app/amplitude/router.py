import os

from datetime import datetime
from fastapi import APIRouter, Query, HTTPException, BackgroundTasks, Depends
from typing import Literal
from fastapi.responses import FileResponse

from app.auth.deps import require_read
from app.amplitude.client import AmplitudeClient
from app.amplitude.export_utils import create_ndjson_zip
from app.config.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.get("/export", response_class=FileResponse)
async def amplitude_export(
    start: str = Query(..., description="Start date (YYYYMMDD)", example="20240201"),
    end: str = Query(..., description="End date (YYYYMMDD)", example="20240207"),
    source: Literal["web", "mobile"] = Query("web"),
    background_tasks: BackgroundTasks = None,
    user=Depends(require_read),
):
    try:
        start_dt = datetime.strptime(start, "%Y%m%d")
        end_dt = datetime.strptime(end, "%Y%m%d")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    client = AmplitudeClient(source=source)
    lines_iterator = client.iter_lines(start_dt, end_dt)

    archive_name = f"amplitude_export_{start}_{end}.zip"
    ndjson_name = f"amplitude_export_{start}_{end}.ndjson"

    zip_path = await create_ndjson_zip(lines_iterator, archive_name, ndjson_name)

    if background_tasks:
        background_tasks.add_task(os.remove, zip_path)

    return FileResponse(
        zip_path,
        filename=archive_name,
        media_type="application/zip",
    )

import os

from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime, timedelta
from typing import Union

from app.amplitude.client import AmplitudeClient
from app.auth.deps import require_write
from app.s3.client import S3Client
from app.etl.orchestrator import process_source, ProcessingInterrupted
from app.etl.schemas import (
    ExportRequest,
    ExportResult,
    S3TransformRequest,
    TmpTableTransformRequest,
)
from app.config.logger import get_logger
from app.amplitude.export_utils import create_ndjson_zip


logger = get_logger(__name__)
router = APIRouter()


def group_dates_by_week(date_from: datetime, date_to: datetime):
    """Return dict {week_key: [dates]} (same as before)."""
    weeks = {}
    current = date_from
    while current <= date_to:
        year, week, _ = current.isocalendar()
        key = f"{year}_week_{week}"
        weeks.setdefault(key, []).append(current)
        current += timedelta(days=1)
    return weeks


@router.post("/amplitude-to-s3", response_model=ExportResult)
async def amplitude_to_s3_export(request: ExportRequest, user=Depends(require_write)):
    try:
        date_from = datetime.strptime(request.date_from, "%Y-%m-%d")
        date_to = datetime.strptime(request.date_to, "%Y-%m-%d")
        if date_from > date_to:
            raise HTTPException(
                status_code=400, detail="date_from must be before date_to"
            )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Date parsing error: {e}")

    weeks = group_dates_by_week(date_from, date_to)
    s3_client = S3Client()
    s3_files = []

    for week_key, week_dates in weeks.items():
        # Week range: first day 00:00 – last day 23:59
        week_start = week_dates[0]
        week_end = week_dates[-1]

        client = AmplitudeClient(source=request.source)
        lines_iterator = client.iter_lines(week_start, week_end)

        archive_name = f"{week_key}.zip"
        ndjson_name = f"{week_key}.ndjson"

        # Create ZIP with one .ndjson file
        zip_path = await create_ndjson_zip(lines_iterator, archive_name, ndjson_name)

        try:
            with open(zip_path, "rb") as f:
                zip_bytes = f.read()
            s3_key = os.path.join(request.s3_dir, archive_name)
            s3_client.put_object(s3_key, zip_bytes, content_type="application/zip")
            s3_files.append(s3_key)
            logger.info(f"Week {week_key} uploaded to {s3_key}")
        finally:
            os.remove(zip_path)  # clean up temp file

    return ExportResult(s3_files=s3_files)


TransformRequest = Union[S3TransformRequest, TmpTableTransformRequest]


@router.post("/user-properties")
def run_user_properties_transform(
    request: TransformRequest, user=Depends(require_write)
):
    try:
        source_type = "amplitude" if request.source == "s3" else "tmp_table"
        params = request.model_dump()

        result = process_source(source_type, params)

        return {"status": "success", "message": "Transformation completed", **result}

    except ProcessingInterrupted as pie:
        logger.warning(
            "Controlled interruption during transformation",
            extra={
                "processed": pie.processed if hasattr(pie, "processed") else None,
                "last_successful_line": pie.last_successful_line,
                "failed_line": pie.failed_line,
                "file_key": pie.file_key,
                "error": pie.message,
            },
        )

        return {
            "status": "interrupted",
            "message": "Processing stopped due to error",
            "error": pie.message,
            "last_successful_line": pie.last_successful_line,
            "failed_line": pie.failed_line,
            "file_key": pie.file_key,
            "processed": getattr(pie, "processed", 0),
        }

    except Exception as e:
        logger.exception("Unexpected error in user-properties transform")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

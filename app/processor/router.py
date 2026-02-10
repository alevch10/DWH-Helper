"""Processor API router: export Amplitude data to S3 by weeks."""

from fastapi import APIRouter, Query, HTTPException, Depends
from app.auth.deps import require_write
from pydantic import BaseModel, Field
from typing import List, Literal
from datetime import datetime, timedelta
import logging
import tempfile
import os

from app.amplitude.client import AmplitudeClient
from app.s3.client import S3Client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/processor", tags=["Processor"])

class ExportRequest(BaseModel):
    date_from: str = Field(..., description="Дата начала (YYYY-MM-DD)", example="2024-01-01")
    date_to: str = Field(..., description="Дата конца (YYYY-MM-DD)", example="2024-01-31")
    s3_dir: str = Field(..., description="Директория внутри бакета S3", example="amplitude_exports/")
    source: Literal["web", "mobile"] = Field("web", description="Источник Amplitude: web или mobile")

class ExportResult(BaseModel):
    s3_files: List[str] = Field(..., description="Список путей файлов, загруженных в S3")

@router.post("/amplitude-to-s3", response_model=ExportResult, summary="Экспортировать данные Amplitude в S3 по неделям")
async def amplitude_to_s3_export(request: ExportRequest, user=Depends(require_write)):
    """
    Экспортировать данные Amplitude за указанный диапазон дат по неделям и загрузить архивы в S3.
    """
    try:
        date_from = datetime.strptime(request.date_from, "%Y-%m-%d")
        date_to = datetime.strptime(request.date_to, "%Y-%m-%d")
        if date_from > date_to:
            raise HTTPException(status_code=400, detail="date_from must be before date_to")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка парсинга дат: {e}")

    s3_client = S3Client()
    s3_files = []
    cur = date_from
    while cur <= date_to:
        week_start = cur
        week_end = min(week_start + timedelta(days=6), date_to)
        # Формат для Amplitude: YYYYMMDD
        start_str = week_start.strftime("%Y%m%d")
        end_str = week_end.strftime("%Y%m%d")
        try:
            amplitude = AmplitudeClient(source=request.source)
            zip_bytes = await amplitude.export(start=f"{start_str}T00", end=f"{end_str}T23")
        except Exception as e:
            logger.error(f"Ошибка экспорта Amplitude за {start_str}-{end_str}: {e}")
            raise HTTPException(status_code=500, detail=f"Ошибка экспорта Amplitude: {e}")
        # Сохраняем zip во временный файл
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            tmp_zip.write(zip_bytes)
            tmp_zip_path = tmp_zip.name
        # Имя файла для S3
        year, week, _ = week_start.isocalendar()
        s3_key = os.path.join(request.s3_dir, f"{year}_week_{week}.zip")
        # Загружаем в S3
        with open(tmp_zip_path, "rb") as f:
            s3_client.put_object(s3_key, f.read(), content_type="application/zip")
        s3_files.append(s3_key)
        os.remove(tmp_zip_path)
        cur = week_end + timedelta(days=1)
    return ExportResult(s3_files=s3_files)

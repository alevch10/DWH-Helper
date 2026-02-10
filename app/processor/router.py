"""Processor API router: export Amplitude data to S3 by weeks."""

from fastapi import APIRouter, Query, HTTPException, Depends
from app.auth.deps import require_write
from pydantic import BaseModel, Field
from typing import List, Literal, Dict
from datetime import datetime, timedelta
import logging
import tempfile
import os
import zipfile
from io import BytesIO

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

def group_dates_by_week(date_from: datetime, date_to: datetime) -> Dict[str, List[datetime]]:
    """Группирует даты по неделям."""
    weeks = {}
    current_date = date_from
    
    while current_date <= date_to:
        year, week_num, _ = current_date.isocalendar()
        week_key = f"{year}_week_{week_num}"
        
        if week_key not in weeks:
            weeks[week_key] = []
        
        weeks[week_key].append(current_date)
        current_date += timedelta(days=1)
    
    return weeks

@router.post("/amplitude-to-s3", response_model=ExportResult, summary="Экспортировать данные Amplitude в S3 по неделям")
async def amplitude_to_s3_export(request: ExportRequest, user=Depends(require_write)):
    """
    Экспортировать данные Amplitude за указанный диапазон дат по неделям и загрузить архивы в S3.
    Данные скачиваются по дням, затем объединяются в zip-архивы по неделям.
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
    
    # Группируем даты по неделям
    weeks = group_dates_by_week(date_from, date_to)
    
    # Обрабатываем каждую неделю
    for week_key, dates in weeks.items():
        try:
            # Создаем временный zip-архив для недели
            zip_buffer = BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                amplitude = AmplitudeClient(source=request.source)
                
                # Скачиваем данные по дням
                for day_date in dates:
                    day_str = day_date.strftime("%Y%m%d")
                    try:
                        # Скачиваем данные за день
                        day_zip_bytes = await amplitude.export(
                            start=f"{day_str}T00", 
                            end=f"{day_str}T23"
                        )
                        
                        # Распаковываем day_zip и добавляем файлы в недельный архив
                        with zipfile.ZipFile(BytesIO(day_zip_bytes), 'r') as day_zip:
                            for file_info in day_zip.infolist():
                                # Читаем содержимое файла
                                file_data = day_zip.read(file_info.filename)
                                # Создаем уникальное имя файла с датой
                                filename_without_ext = os.path.splitext(file_info.filename)[0]
                                ext = os.path.splitext(file_info.filename)[1]
                                new_filename = f"{day_str}_{filename_without_ext}{ext}"
                                # Добавляем файл в недельный архив
                                zip_file.writestr(new_filename, file_data)
                                
                    except Exception as e:
                        logger.error(f"Ошибка экспорта Amplitude за день {day_str}: {e}")
                        raise HTTPException(
                            status_code=500, 
                            detail=f"Ошибка экспорта Amplitude за день {day_str}: {e}"
                        )
            
            # Загружаем недельный архив в S3
            s3_key = os.path.join(request.s3_dir, f"{week_key}.zip")
            s3_client.put_object(
                s3_key, 
                zip_buffer.getvalue(), 
                content_type="application/zip"
            )
            s3_files.append(s3_key)
            
            logger.info(f"Успешно загружена неделя {week_key} с {len(dates)} днями")
            
        except Exception as e:
            logger.error(f"Ошибка обработки недели {week_key}: {e}")
            raise HTTPException(
                status_code=500, 
                detail=f"Ошибка обработки недели {week_key}: {e}"
            )
    
    return ExportResult(s3_files=s3_files)
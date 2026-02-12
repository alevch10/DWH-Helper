import os
import zipfile

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Union
from datetime import datetime, timedelta
from io import BytesIO

from app.amplitude.client import AmplitudeClient
from app.auth.deps import require_write
from app.s3.client import S3Client
from app.processor.orchestrator import process_source, ProcessingInterrupted
from app.processor.schemas import (
    S3TransformRequest,
    TmpTableTransformRequest,
    TransformResponse,
    ExportRequest,
    ExportResult,
)
from app.config.logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/processor", tags=["Processor"])


def group_dates_by_week(
    date_from: datetime, date_to: datetime
) -> Dict[str, List[datetime]]:
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


@router.post(
    "/amplitude-to-s3",
    response_model=ExportResult,
    summary="Экспортировать данные Amplitude в S3 по неделям",
)
async def amplitude_to_s3_export(request: ExportRequest, user=Depends(require_write)):
    """
    Экспортировать данные Amplitude за указанный диапазон дат по неделям и загрузить архивы в S3.
    Данные скачиваются по дням, затем объединяются в zip-архивы по неделям.
    """
    try:
        date_from = datetime.strptime(request.date_from, "%Y-%m-%d")
        date_to = datetime.strptime(request.date_to, "%Y-%m-%d")
        if date_from > date_to:
            raise HTTPException(
                status_code=400, detail="date_from must be before date_to"
            )
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

            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                amplitude = AmplitudeClient(source=request.source)

                # Скачиваем данные по дням
                for day_date in dates:
                    day_str = day_date.strftime("%Y%m%d")
                    try:
                        # Скачиваем данные за день
                        day_zip_bytes = await amplitude.export(
                            start=f"{day_str}T00", end=f"{day_str}T23"
                        )

                        # Распаковываем day_zip и добавляем файлы в недельный архив
                        with zipfile.ZipFile(BytesIO(day_zip_bytes), "r") as day_zip:
                            for file_info in day_zip.infolist():
                                # Читаем содержимое файла
                                file_data = day_zip.read(file_info.filename)
                                # Создаем уникальное имя файла с датой
                                filename_without_ext = os.path.splitext(
                                    file_info.filename
                                )[0]
                                ext = os.path.splitext(file_info.filename)[1]
                                new_filename = f"{day_str}_{filename_without_ext}{ext}"
                                # Добавляем файл в недельный архив
                                zip_file.writestr(new_filename, file_data)

                    except Exception as e:
                        logger.error(
                            f"Ошибка экспорта Amplitude за день {day_str}: {e}"
                        )
                        raise HTTPException(
                            status_code=500,
                            detail=f"Ошибка экспорта Amplitude за день {day_str}: {e}",
                        )

            # Загружаем недельный архив в S3
            s3_key = os.path.join(request.s3_dir, f"{week_key}.zip")
            s3_client.put_object(
                s3_key, zip_buffer.getvalue(), content_type="application/zip"
            )
            s3_files.append(s3_key)

            logger.info(f"Успешно загружена неделя {week_key} с {len(dates)} днями")

        except Exception as e:
            logger.error(f"Ошибка обработки недели {week_key}: {e}")
            raise HTTPException(
                status_code=500, detail=f"Ошибка обработки недели {week_key}: {e}"
            )

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
                "processed": pie.processed
                if hasattr(pie, "processed")
                else None,  # если добавишь атрибут
                "last_successful_line": pie.last_successful_line,
                "failed_line": pie.failed_line,
                "file_key": pie.file_key,
                "error": pie.message,
            },
        )

        # Возвращаем 200 + полные детали → клиент может повторить запуск
        return {
            "status": "interrupted",
            "message": "Processing stopped due to error",
            "error": pie.message,
            "last_successful_line": pie.last_successful_line,
            "failed_line": pie.failed_line,
            "file_key": pie.file_key,
            "processed": getattr(pie, "processed", 0),  # если добавишь в исключение
        }

    except Exception as e:
        logger.exception("Unexpected error in user-properties transform")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

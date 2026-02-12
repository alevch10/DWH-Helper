"""Amplitude API router for FastAPI."""

import zipfile
import gzip
import tempfile
import os

from fastapi import APIRouter, Query, HTTPException, BackgroundTasks, Depends
from app.auth.deps import require_read
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Literal
from datetime import datetime

from app.amplitude.client import AmplitudeClient
from app.config.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/amplitude", tags=["Amplitude"])


class AmplitudeExportRequest(BaseModel):
    start: str = Query(..., description="Start date (YYYYMMDD)", example="20240201")
    end: str = Query(..., description="End date (YYYYMMDD)", example="20240207")
    source: Literal["web", "mobile"] = Query("web", description="Source: web or mobile")


@router.get(
    "/export", response_class=FileResponse, summary="Экспорт данных из Amplitude"
)
async def amplitude_export(
    start: str = Query(..., description="Дата начала (YYYYMMDD)", example="20240201"),
    end: str = Query(..., description="Дата конца (YYYYMMDD)", example="20240207"),
    source: Literal["web", "mobile"] = Query(
        "web", description="Источник: web или mobile"
    ),
    background_tasks: BackgroundTasks = None,
    user=Depends(require_read),
):
    """
    Экспорт данных из Amplitude за указанный диапазон дат (zip архив с .gz файлами внутри).
    - start: начало периода (YYYYMMDD), часы по умолчанию 00
    - end: конец периода (YYYYMMDD), часы по умолчанию 23
    - source: web или mobile (выбор пары ключей)
    """
    try:
        # Формируем параметры Amplitude (часы по умолчанию)
        start_fmt = f"{start}T00"
        end_fmt = f"{end}T23"
        client = AmplitudeClient(source=source)
        zip_bytes = await client.export(start=start_fmt, end=end_fmt)
    except Exception as e:
        logger.error(f"Ошибка экспорта Amplitude: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка экспорта Amplitude: {e}")

    # Распаковка zip и обработка .gz файлов
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "amplitude.zip")
        with open(zip_path, "wb") as f:
            f.write(zip_bytes)
        with zipfile.ZipFile(zip_path) as zip_ref:
            gz_files = [name for name in zip_ref.namelist() if name.endswith(".gz")]
            if not gz_files:
                raise HTTPException(
                    status_code=400, detail="Нет .gz файлов в архиве Amplitude"
                )
            all_lines = []
            for gz_name in gz_files:
                with zip_ref.open(gz_name) as gz_file:
                    gz_content = gz_file.read()
                    json_content = gzip.decompress(gz_content)
                    lines = json_content.decode("utf-8").splitlines()
                    all_lines.extend(lines)
        # Формируем итоговый файл за неделю
        dt = datetime.strptime(start, "%Y%m%d")
        year, week, _ = dt.isocalendar()
        out_name = f"{year}_week_{week}.json"
        out_json_path = os.path.join(tmpdir, out_name)
        with open(out_json_path, "w", encoding="utf-8") as f:
            f.write("[\n" + ",\n".join(all_lines) + "\n]")
        # Архивируем результат
        out_zip_path = os.path.join(tmpdir, f"{year}_week_{week}.zip")
        with zipfile.ZipFile(out_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(out_json_path, arcname=out_name)
        # Копируем архив во временный файл вне tmpdir
        import tempfile as _tempfile
        import shutil as _shutil

        temp_zip = _tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        _shutil.copyfile(out_zip_path, temp_zip.name)
        temp_zip.close()
        # Удалить файл после отправки
        if background_tasks is not None:
            background_tasks.add_task(os.remove, temp_zip.name)
        return FileResponse(
            temp_zip.name,
            filename=f"{year}_week_{week}.zip",
            media_type="application/zip",
        )

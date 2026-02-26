import asyncio
import csv
import io
import tempfile
import zipfile
import os
import shutil
from pathlib import Path
from typing import List

from fastapi import HTTPException

from app.yandex_metrika.client import MetrikaClient
from app.yandex_metrika.schemas import LogRequestEvaluation


async def process_part_streaming(
    client: MetrikaClient,
    counter_id: int,
    request_id: int,
    part_number: int,
    csv_writer,
    clean_headers: List[str],
):
    """
    Потоково обрабатывает одну часть: читает чанки, накапливает строки, парсит и пишет в CSV.
    """
    buffer = b""
    async for chunk in client.download_part_stream(counter_id, request_id, part_number):
        buffer += chunk
        while b"\n" in buffer:
            line_bytes, buffer = buffer.split(b"\n", 1)
            line = line_bytes.decode("utf-8")
            if not line.strip():
                continue
            # Используем csv.reader для корректного разбора TSV (учитывает кавычки)
            reader = csv.reader([line], delimiter="\t")
            try:
                values = next(reader)
            except StopIteration:
                continue
            if len(values) < len(clean_headers):
                values.extend([""] * (len(clean_headers) - len(values)))
            row_dict = dict(zip(clean_headers, values))
            # Запись в CSV (синхронный writer)
            csv_writer.writerow([row_dict.get(h) for h in clean_headers])
    # Остаток буфера (последняя строка без завершающего \n)
    if buffer:
        line = buffer.decode("utf-8")
        if line.strip():
            reader = csv.reader([line], delimiter="\t")
            values = next(reader)
            if len(values) < len(clean_headers):
                values.extend([""] * (len(clean_headers) - len(values)))
            row_dict = dict(zip(clean_headers, values))
            csv_writer.writerow([row_dict.get(h) for h in clean_headers])


async def generate_report(
    token: str,
    counter_id: int,
    date1: str,
    date2: str,
    source: str,
    fields: str,
) -> bytes:
    """
    Основная логика подготовки отчета с потоковой обработкой и записью на диск.
    Возвращает ZIP-архив с CSV-файлом.
    """
    client = MetrikaClient(token)
    temp_dir = None
    try:
        # 1. Оценка возможности создания запроса
        eval_data = await client.evaluate_logrequest(
            counter_id, date1, date2, fields, source
        )
        eval_obj = LogRequestEvaluation(**eval_data["log_request_evaluation"])
        if not eval_obj.possible:
            raise HTTPException(
                status_code=400,
                detail="Невозможно создать запрос логов для указанных параметров",
            )

        # 2. Создание запроса
        create_data = await client.create_logrequest(
            counter_id, date1, date2, fields, source
        )
        request_id = create_data["log_request"]["request_id"]

        # 3. Ожидание обработки (опрос каждые 15 секунд)
        while True:
            await asyncio.sleep(15)
            info_data = await client.get_logrequest_info(counter_id, request_id)
            status = info_data["log_request"]["status"]
            if status == "processed":
                parts = info_data["log_request"].get("parts", [])
                break
            elif status == "canceled":
                raise HTTPException(
                    status_code=400, detail="Запрос логов был отменен Яндекс.Метрикой"
                )
            # иначе продолжаем ждать (created, processing)

        if not parts:
            raise HTTPException(status_code=500, detail="Нет частей для скачивания")

        # 4. Создаём временную директорию для CSV-файла
        temp_dir = tempfile.mkdtemp()
        csv_path = Path(temp_dir) / "report.csv"

        # Используем синхронный open (не aiofiles) для совместимости с csv.writer
        with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, delimiter="\t")

            # Заголовки берём из первой части
            first_part_content = await client.download_part(counter_id, request_id, 0)
            first_lines = first_part_content.decode("utf-8").splitlines()
            if not first_lines:
                raise HTTPException(status_code=500, detail="Первая часть логов пуста")
            headers = first_lines[0].split("\t")
            # Убираем префикс "ym:pv:" и заменяем "from" на "from_"
            clean_headers = [
                h.replace("ym:pv:", "").replace("from", "from_") for h in headers
            ]
            writer.writerow(clean_headers)

            # Обрабатываем остаток первой части (строки после заголовка)
            for line in first_lines[1:]:
                if not line.strip():
                    continue
                reader = csv.reader([line], delimiter="\t")
                values = next(reader)
                if len(values) < len(clean_headers):
                    values.extend([""] * (len(clean_headers) - len(values)))
                row_dict = dict(zip(clean_headers, values))
                writer.writerow([row_dict.get(h) for h in clean_headers])

            # Обрабатываем остальные части потоково
            for part in parts[1:]:
                part_number = part["part_number"]
                await process_part_streaming(
                    client, counter_id, request_id, part_number, writer, clean_headers
                )

        # 5. Создаём ZIP-архив в памяти
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname="report.csv")
        zip_buffer.seek(0)
        return zip_buffer.getvalue()

    finally:
        await client.close()
        # Удаляем временную директорию
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

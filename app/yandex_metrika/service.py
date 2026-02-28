# app/yandex_metrika/service.py

import asyncio
import csv
import io
import tempfile
import zipfile
import os
import shutil
from pathlib import Path
from typing import Callable

from fastapi import HTTPException

from app.yandex_metrika.client import MetrikaClient
from app.yandex_metrika.schemas import LogRequestEvaluation


async def process_part_streaming(
    client: MetrikaClient,
    counter_id: int,
    request_id: int,
    part_number: int,
    process_line: Callable[[str], None],
) -> None:
    """
    Потоково обрабатывает одну часть: читает чанки, накапливает строки,
    передаёт каждую полную строку в process_line для фильтрации и записи.
    """
    buffer = b""
    async for chunk in client.download_part_stream(counter_id, request_id, part_number):
        buffer += chunk
        while b"\n" in buffer:
            line_bytes, buffer = buffer.split(b"\n", 1)
            line = line_bytes.decode("utf-8")
            if not line.strip():
                continue
            process_line(line)
    # Остаток буфера (последняя строка без завершающего \n)
    if buffer:
        line = buffer.decode("utf-8")
        if line.strip():
            process_line(line)


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

        # Загружаем первую часть, чтобы получить заголовки
        first_part_content = await client.download_part(counter_id, request_id, 0)
        first_lines = first_part_content.decode("utf-8").splitlines()
        if not first_lines:
            raise HTTPException(status_code=500, detail="Первая часть логов пуста")

        # Сохраняем исходную строку заголовков (с префиксами) для фильтрации повторных заголовков
        header_line_raw = first_lines[0]
        headers = header_line_raw.split("\t")
        # Очищаем заголовки: убираем "ym:pv:", заменяем "from" на "from_"
        clean_headers = [
            h.replace("ym:pv:", "").replace("from", "from_") for h in headers
        ]

        # Функция для обработки одной строки: если это не заголовок, парсим и пишем в CSV
        def process_line(line: str) -> None:
            if line.strip() == header_line_raw:
                return  # пропускаем повторяющиеся заголовки
            reader = csv.reader([line], delimiter="\t")
            try:
                values = next(reader)
            except StopIteration:
                return
            if len(values) < len(clean_headers):
                values.extend([""] * (len(clean_headers) - len(values)))
            row_dict = dict(zip(clean_headers, values))
            writer.writerow([row_dict.get(h) for h in clean_headers])

        # Открываем CSV-файл (синхронно) и пишем
        with open(csv_path, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, delimiter="\t")
            # Записываем заголовки (один раз)
            writer.writerow(clean_headers)

            # Обрабатываем остаток первой части (строки после заголовка)
            for line in first_lines[1:]:
                process_line(line)

            # Потоково обрабатываем остальные части
            for part in parts[1:]:
                part_number = part["part_number"]
                await process_part_streaming(
                    client, counter_id, request_id, part_number, process_line
                )

        # 5. Создаём ZIP-архив в памяти
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname="report.csv")
        zip_buffer.seek(0)
        try:
            await client.clean_logrequest(counter_id, request_id)
        except Exception as e:
            print(f"Не удалось удалить запрос логов {request_id}: {e}")

        return zip_buffer.getvalue()

    finally:
        await client.close()
        # Удаляем временную директорию
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

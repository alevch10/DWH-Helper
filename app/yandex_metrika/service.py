import asyncio
import csv
import io
import zipfile
from typing import List, AsyncGenerator
from fastapi import HTTPException

from .client import MetrikaClient
from .schemas import MetrikaHitRow, LogRequestEvaluation, LogRequest


async def generate_report(
    token: str,
    counter_id: int,
    date1: str,
    date2: str,
    source: str,
    fields: str,
    return_as_zip: bool = False,
) -> List[MetrikaHitRow] | bytes:
    """
    Основная логика подготовки отчета.
    Если return_as_zip=True, возвращает ZIP-архив с CSV.
    Иначе возвращает список объектов MetrikaHitRow.
    """
    client = MetrikaClient(token)
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

        # 3. Ожидание обработки
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
            # иначе продолжаем ждать (created, processing и т.д.)

        # 4. Загрузка и парсинг частей
        all_rows: List[MetrikaHitRow] = []
        for part in parts:
            part_number = part["part_number"]
            content = await client.download_part(counter_id, request_id, part_number)
            text = content.decode("utf-8")
            lines = text.splitlines()
            if not lines:
                continue
            headers = lines[0].split("\t")
            # Убираем префикс "ym:pv:" из заголовков для соответствия полям модели
            clean_headers = [h.replace("ym:pv:", "") for h in headers]
            # Заменяем "from" на "from_"
            clean_headers = ["from_" if h == "from" else h for h in clean_headers]

            for line in lines[1:]:
                if not line.strip():
                    continue
                values = line.split("\t")
                if len(values) < len(clean_headers):
                    values.extend([""] * (len(clean_headers) - len(values)))
                row_dict = dict(zip(clean_headers, values))
                # Заменяем пустые строки на None
                row_dict = {k: v if v != "" else None for k, v in row_dict.items()}
                try:
                    row = MetrikaHitRow.model_validate(row_dict)
                    all_rows.append(row)
                except Exception as e:
                    # Логируем ошибку, но продолжаем
                    print(f"Error parsing row: {e}, data: {row_dict}")

        if return_as_zip:
            # Создаем ZIP с CSV
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer, delimiter="\t")
                # Заголовки CSV – имена полей модели
                field_names = list(MetrikaHitRow.model_fields.keys())
                writer.writerow(field_names)
                for row in all_rows:
                    writer.writerow([getattr(row, f) for f in field_names])
                zf.writestr("report.csv", csv_buffer.getvalue())
            zip_buffer.seek(0)
            return zip_buffer.getvalue()
        else:
            return all_rows

    finally:
        await client.close()

import asyncio
import csv
import io
import tempfile
import zipfile
import os
import shutil

from pathlib import Path
from typing import Callable, List
from fastapi import HTTPException

from app.config.logger import get_logger
from app.yandex_metrika.client import MetrikaClient
from app.yandex_metrika.schemas import LogRequestEvaluation, MetrikaHitRow


logger = get_logger(__name__)


async def process_part_streaming(
    client: MetrikaClient,
    counter_id: int,
    request_id: int,
    part_number: int,
    process_line: Callable[[str], None],
) -> None:
    """
    Потоково скачивает и обрабатывает одну часть логов.
    """
    logger.info(
        "Начало потоковой загрузки части #%d (request_id=%d)", part_number, request_id
    )

    buffer = b""
    chunk_count = 0
    line_count = 0

    async for chunk in client.download_part_stream(counter_id, request_id, part_number):
        chunk_count += 1
        buffer += chunk
        while b"\n" in buffer:
            line_bytes, buffer = buffer.split(b"\n", 1)
            line = line_bytes.decode("utf-8", errors="replace")
            if not line.strip():
                continue
            process_line(line)
            line_count += 1

    if buffer:
        line = buffer.decode("utf-8", errors="replace")
        if line.strip():
            process_line(line)
            line_count += 1

    logger.info(
        "Часть #%d загружена: %d чанков, обработано строк: %d",
        part_number,
        chunk_count,
        line_count,
    )


async def _initialize_log_request(
    client: MetrikaClient,
    counter_id: int,
    date1: str,
    date2: str,
    fields: str,
    source: str,
) -> tuple[int, List[dict]]:
    """Оценка → создание → ожидание обработки."""
    logger.info(
        "Инициализация запроса логов: %s → %s, source=%s, counter=%d",
        date1,
        date2,
        source,
        counter_id,
    )

    eval_data = await client.evaluate_logrequest(
        counter_id, date1, date2, fields, source
    )
    eval_obj = LogRequestEvaluation(**eval_data["log_request_evaluation"])

    if not eval_obj.possible:
        logger.error("Запрос невозможен: %s", eval_data)
        raise ValueError("Невозможно создать запрос логов для указанных параметров")

    create_data = await client.create_logrequest(
        counter_id, date1, date2, fields, source
    )
    request_id = create_data["log_request"]["request_id"]
    logger.info("Создан запрос: request_id=%d", request_id)

    attempt = 0
    while True:
        await asyncio.sleep(15)
        attempt += 1
        info_data = await client.get_logrequest_info(counter_id, request_id)
        status = info_data["log_request"]["status"]

        if status == "processed":
            parts = info_data["log_request"].get("parts", [])
            logger.info(
                "Запрос обработан: request_id=%d, частей: %d", request_id, len(parts)
            )
            break
        elif status == "canceled":
            logger.warning("Запрос отменён Метрикой: request_id=%d", request_id)
            raise ValueError("Запрос логов был отменён Яндекс.Метрикой")

        if attempt % 4 == 0:  # каждую минуту
            logger.info("Ожидание обработки... статус=%s (попытка %d)", status, attempt)

    if not parts:
        logger.error("Нет частей после обработки: request_id=%d", request_id)
        raise ValueError("Нет частей для скачивания после обработки")

    return request_id, parts


def _parse_line_to_dict(
    line: str,
    header_line_raw: str,
    clean_headers: List[str],
) -> dict[str, str] | None:
    if line.strip() == header_line_raw or not line.strip():
        return None

    reader = csv.reader([line], delimiter="\t")
    try:
        values = next(reader)
    except Exception as e:
        logger.warning("Ошибка парсинга TSV-строки: %s", e)
        return None

    if len(values) < len(clean_headers):
        values.extend([""] * (len(clean_headers) - len(values)))

    return dict(zip(clean_headers, values))


async def generate_report(
    token: str,
    counter_id: int,
    date1: str,
    date2: str,
    source: str,
    fields: str,
) -> bytes:
    """
    Формирует ZIP-архив с report.csv для API-ответа.
    """
    client = MetrikaClient(token)
    temp_dir = None
    processed_lines = 0

    try:
        request_id, parts = await _initialize_log_request(
            client, counter_id, date1, date2, fields, source
        )

        temp_dir = tempfile.mkdtemp()
        csv_path = Path(temp_dir) / "report.csv"
        logger.info("Создан временный CSV: %s", csv_path)

        first_part_content = await client.download_part(counter_id, request_id, 0)
        first_lines = first_part_content.decode("utf-8", errors="replace").splitlines()
        if not first_lines:
            raise HTTPException(500, "Первая часть логов пуста")

        header_line_raw = first_lines[0]
        headers = header_line_raw.split("\t")
        clean_headers = [
            h.replace("ym:pv:", "").replace("from", "from_") for h in headers
        ]
        logger.info("Получены заголовки (%d полей)", len(clean_headers))

        def process_line(line: str) -> None:
            nonlocal processed_lines
            row_dict = _parse_line_to_dict(line, header_line_raw, clean_headers)
            if row_dict:
                writer.writerow([row_dict.get(h, "") for h in clean_headers])
                processed_lines += 1

        with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(clean_headers)

            for line in first_lines[1:]:
                process_line(line)

            for part in parts[1:]:
                await process_part_streaming(
                    client, counter_id, request_id, part["part_number"], process_line
                )

        logger.info("CSV сформирован: строк данных ≈ %d", processed_lines)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname="report.csv")
        zip_buffer.seek(0)

        try:
            await client.clean_logrequest(counter_id, request_id)
            logger.info("Запрос очищен: request_id=%d", request_id)
        except Exception as e:
            logger.warning("Не удалось очистить запрос %d: %s", request_id, e)

        return zip_buffer.getvalue()

    except ValueError as e:
        logger.error("Ошибка подготовки отчёта: %s", e)
        raise HTTPException(400, str(e)) from e
    except Exception as e:
        logger.exception("Неожиданная ошибка при генерации отчёта")
        raise
    finally:
        await client.close()
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.debug("Временная директория удалена: %s", temp_dir)


async def get_metrika_hits(
    token: str,
    counter_id: int,
    date1: str,
    date2: str,
    source: str,
    fields: str,
) -> List[MetrikaHitRow]:
    """
    Возвращает список объектов MetrikaHitRow для внутреннего использования.
    """
    client = MetrikaClient(token)
    hits: List[MetrikaHitRow] = []
    invalid_rows = 0

    try:
        logger.info("Инициализация запроса данных ЯМ")
        request_id, parts = await _initialize_log_request(
            client, counter_id, date1, date2, fields, source
        )

        first_part_content = await client.download_part(counter_id, request_id, 0)
        first_lines = first_part_content.decode("utf-8", errors="replace").splitlines()
        if not first_lines:
            logger.warning("Первая часть пуста → возвращаем пустой список")
            return hits

        header_line_raw = first_lines[0]
        headers = header_line_raw.split("\t")
        clean_headers = [
            h.replace("ym:pv:", "").replace("from", "from_") for h in headers
        ]

        def process_line(line: str) -> None:
            nonlocal invalid_rows
            row_dict = _parse_line_to_dict(line, header_line_raw, clean_headers)
            if not row_dict:
                return
            try:
                hit = MetrikaHitRow.model_validate(row_dict)
                hits.append(hit)
            except Exception as e:
                invalid_rows += 1
                if invalid_rows <= 5:
                    logger.warning(
                        "Ошибка валидации строки: %s\nСтрока: %s", e, line[:200]
                    )
                elif invalid_rows == 6:
                    logger.warning(
                        "... и ещё %d некорректных строк (подробности опущены)",
                        invalid_rows,
                    )

        for line in first_lines[1:]:
            process_line(line)

        for part in parts[1:]:
            await process_part_streaming(
                client, counter_id, request_id, part["part_number"], process_line
            )

        try:
            await client.clean_logrequest(counter_id, request_id)
            logger.info("Запрос очищен: request_id=%d", request_id)
        except Exception as e:
            logger.warning("Не удалось очистить запрос %d: %s", request_id, e)

        logger.info(
            "Обработано хитов: %d (отклонено по валидации: %d)", len(hits), invalid_rows
        )
        return hits

    except Exception as e:
        logger.exception("Ошибка при получении хитов из Метрики")
        raise
    finally:
        await client.close()

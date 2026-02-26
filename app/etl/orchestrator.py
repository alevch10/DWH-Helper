import json
from typing import Dict, List, Optional, Set, Any, Tuple
from uuid import UUID
from datetime import datetime, timedelta
from fastapi import HTTPException

from app.etl.transformer import transform_single_record, SourceType
from app.db.schemas import (
    PermanentUserProperties,
    ChangeableUserProperties,
)
from app.db.repository import get_repository, DBRepository
from app.config.settings import settings
from app.config.logger import get_logger

logger = get_logger(__name__)


def log_bads(errors: List[Dict[str, Any]]):
    for err in errors:
        logger.error("Transformation error: %s", err)


def compare_changeable(
    old: Optional[ChangeableUserProperties], new: ChangeableUserProperties
) -> bool:
    if old is None:
        return True

    exclude = {"uuid", "event_time", "session_id"}
    return any(
        getattr(old, f) != getattr(new, f)
        for f in ChangeableUserProperties.model_fields
        if f not in exclude
    )


class ProcessingInterrupted(Exception):
    """Специальное исключение для контролируемого прерывания обработки"""

    def __init__(
        self, message, last_successful_line=None, failed_line=None, file_key=None
    ):
        self.message = message
        self.last_successful_line = last_successful_line
        self.failed_line = failed_line
        self.file_key = file_key
        super().__init__(message)


def _format_transform_error(errors: List[Dict[str, Any]]) -> str:
    """
    Берёт первую ошибку из списка transform_errors и возвращает читаемое сообщение.
    Если ошибок несколько, добавляет информацию о количестве.
    """
    if not errors:
        return "Неизвестная ошибка трансформации"

    MAX_ERRORS_TO_SHOW = 2
    details = []
    for i, e in enumerate(errors[:MAX_ERRORS_TO_SHOW]):
        key = e.get("key", "<неизвестный ключ>")
        value = e.get("value", "<неизвестное значение>")
        reason = e.get("reason", "без причины")
        details.append(f"'{key}' = {value} ({reason})")

    msg = "; ".join(details)
    if len(errors) > MAX_ERRORS_TO_SHOW:
        msg += f" и ещё {len(errors) - MAX_ERRORS_TO_SHOW} ошибка(ок)"
    return f"Ошибка трансформации: {msg}"


def process_source(source_type: SourceType, params: Dict[str, Any]) -> Dict[str, Any]:
    repo = get_repository()
    logger.info(f"ETL process started: source={source_type}, params={params}")

    # --- Предзагрузка кэшей (один раз на весь процесс) ---
    logger.info("Preloading existing permanent ehr_ids...")
    existing_permanent: Set[int] = repo.get_all_permanent_ehr_ids()
    logger.info(f"Loaded {len(existing_permanent)} existing permanent ehr_ids")

    logger.info("Preloading latest changeable records...")
    all_ehr_for_change = list(existing_permanent) + [None]
    last_change: Dict[Optional[int], ChangeableUserProperties] = (
        repo.get_latest_changeable_for_ehrs(all_ehr_for_change)
    )
    logger.info(f"Loaded {len(last_change)} latest changeable records")

    processed_total = 0
    errors_total = 0
    batch_size = settings.etl.batch_size

    # Вспомогательная функция для обработки одного дня (или всего файла для amplitude)
    def process_day(day_date: datetime, rows_or_lines, source_type: str, **kwargs):
        nonlocal processed_total, errors_total, existing_permanent, last_change

        pending_permanent: List[Dict[str, Any]] = []
        pending_changeable: List[Dict[str, Any]] = []
        batch_uuids: List[UUID] = []  # только для tmp_table

        def flush():
            """Вставляет накопленные батчи в БД, используя только локальные кэши."""
            nonlocal \
                pending_permanent, \
                pending_changeable, \
                existing_permanent, \
                last_change

            # --- Вставка permanent ---
            if pending_permanent:
                to_insert = []
                for p in pending_permanent:
                    eid = p["ehr_id"]
                    if eid not in existing_permanent:
                        to_insert.append(p)
                if to_insert:
                    inserted_ids, batches = repo.insert_batch(
                        table="permanent_user_properties",
                        rows=to_insert,
                        on_conflict="DO NOTHING",
                        conflict_target="(ehr_id)",
                        returning_column="ehr_id",
                    )
                    logger.info(
                        f"Inserted {len(inserted_ids)} permanent records in {batches} batches"
                    )
                    for eid in inserted_ids:
                        existing_permanent.add(int(eid))
                pending_permanent.clear()

            # --- Вставка changeable ---
            if pending_changeable:
                to_insert = []
                for c in pending_changeable:
                    eid = c["ehr_id"]
                    try:
                        new_rec = ChangeableUserProperties(**c)
                    except Exception:
                        continue
                    old = last_change.get(eid)
                    if compare_changeable(old, new_rec):
                        to_insert.append(c)
                        last_change[eid] = new_rec
                if to_insert:
                    inserted_ids, batches = repo.insert_batch(
                        table="changeable_user_properties",
                        rows=to_insert,
                        returning_column="uuid",
                    )
                    logger.info(
                        f"Inserted {len(inserted_ids)} changeable records in {batches} batches"
                    )
                pending_changeable.clear()

        try:
            if source_type == "amplitude":
                # rows_or_lines — это список строк файла
                for line_idx, line in enumerate(rows_or_lines):
                    try:
                        raw_record = json.loads(line.strip())
                    except json.JSONDecodeError as e:
                        flush()
                        raise ProcessingInterrupted(
                            f"Невалидный JSON на строке {line_idx}",
                            last_successful_line=line_idx - 1,
                            failed_line=line_idx,
                            file_key=kwargs.get("file_key"),
                        )

                    permanent, changeable, transform_errors = transform_single_record(
                        raw_record, source_type="amplitude"
                    )

                    if transform_errors:
                        log_bads(transform_errors)
                        flush()
                        error_msg = _format_transform_error(transform_errors)
                        raise ProcessingInterrupted(
                            error_msg,
                            last_successful_line=line_idx - 1,
                            failed_line=line_idx,
                            file_key=kwargs.get("file_key"),
                        )

                    if permanent:
                        pending_permanent.append(permanent.model_dump())
                    if changeable:
                        pending_changeable.append(changeable.model_dump())

                    processed_total += 1

                    if (
                        len(pending_permanent) >= batch_size
                        or len(pending_changeable) >= batch_size
                    ):
                        flush()

                flush()
                return

            elif source_type == "tmp_table":
                # rows_or_lines — это список записей из БД за день
                for row in rows_or_lines:
                    raw_record = dict(row)

                    permanent, changeable, transform_errors = transform_single_record(
                        raw_record, source_type="tmp_table"
                    )

                    if transform_errors:
                        log_bads(transform_errors)
                        flush()
                        error_msg = _format_transform_error(transform_errors)
                        raise ProcessingInterrupted(
                            f"Ошибка трансформации для записи {raw_record.get('uuid')}: {error_msg}"
                        )

                    if permanent:
                        pending_permanent.append(permanent.model_dump())
                    if changeable:
                        pending_changeable.append(changeable.model_dump())
                    batch_uuids.append(raw_record["uuid"])

                    processed_total += 1

                    if (
                        len(pending_permanent) >= batch_size
                        or len(pending_changeable) >= batch_size
                    ):
                        flush()
                        if batch_uuids:
                            repo.update_migrated_batch(batch_uuids, migrated=True)
                            batch_uuids.clear()

                flush()
                if batch_uuids:
                    repo.update_migrated_batch(batch_uuids, migrated=True)

        except Exception as e:
            # Любая ошибка внутри дня прерывает весь процесс
            logger.error(f"Error processing day {day_date}: {e}")
            raise

    try:
        if source_type == "amplitude":
            from app.s3.client import S3Client

            s3 = S3Client()
            bucket = params["bucket"]
            prefix = params["prefix"]
            start_after_idx = int(params.get("start_after", 0))

            try:
                content = s3.get_object(bucket, prefix).decode("utf-8")
                lines = content.splitlines()
                logger.info(f"Loaded file {prefix} from S3, {len(lines)} lines")
            except Exception as e:
                raise ProcessingInterrupted(
                    f"Не удалось прочитать файл S3: {str(e)}", file_key=prefix
                )

            if start_after_idx >= len(lines):
                logger.info("Start index beyond file length, nothing to process")
                return {
                    "processed": 0,
                    "errors": 0,
                    "last_successful_line": str(len(lines) - 1),
                }

            process_day(
                day_date=datetime.now(),
                rows_or_lines=lines[start_after_idx:],
                source_type="amplitude",
                file_key=prefix,
            )
            logger.info(f"Amplitude ETL finished. Total processed: {processed_total}")

        elif source_type == "tmp_table":
            start_date = params["start_date"]
            # Интервал игнорируем – обрабатываем все дни подряд, пока есть данные
            current_day = datetime.fromisoformat(start_date)

            while True:
                next_day = current_day + timedelta(days=1)
                logger.info(f"Processing day: {current_day.date()}")

                query = """
                    SELECT * FROM tmp_user_properties
                    WHERE migrated = %s
                      AND event_time >= %s
                      AND event_time < %s
                    ORDER BY event_time
                """
                rows = repo.execute(query, (False, current_day, next_day))
                logger.info(f"Selected {len(rows)} rows for day {current_day.date()}")

                if not rows:
                    logger.info(f"No more data after {current_day.date()}. Finishing.")
                    break

                process_day(
                    day_date=current_day, rows_or_lines=rows, source_type="tmp_table"
                )

                current_day = next_day

            logger.info(
                f"Tmp_table ETL finished. Total processed: {processed_total}, Total errors: {errors_total}"
            )

        return {
            "status": "completed",
            "processed": processed_total,
            "errors": errors_total,
            "last_successful_line": None,
        }

    except ProcessingInterrupted as pie:
        logger.error("Обработка прервана: %s", pie.message)
        response = {
            "status": "interrupted",
            "processed": processed_total,
            "errors": errors_total + 1,
            "error_message": pie.message,
        }
        if source_type == "amplitude":
            response.update(
                {
                    "last_successful_line": str(pie.last_successful_line)
                    if pie.last_successful_line is not None
                    else None,
                    "failed_line": str(pie.failed_line)
                    if pie.failed_line is not None
                    else None,
                    "file_key": pie.file_key,
                }
            )
        raise HTTPException(status_code=200, detail=response)

    except Exception as e:
        logger.exception("Неожиданная ошибка при обработке")
        raise HTTPException(500, f"Критическая ошибка: {str(e)}")

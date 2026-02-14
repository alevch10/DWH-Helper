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

    MAX_ERRORS_TO_SHOW = 10
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


def _get_date_range(start_date: str, interval: str) -> Tuple[datetime, datetime]:
    """
    Преобразует строку даты и интервал в начальную и конечную дату (end_date исключительно).
    Поддерживает день, неделю, месяц.
    """
    start = datetime.fromisoformat(start_date)
    if interval == "day":
        end = start + timedelta(days=1)
    elif interval == "week":
        end = start + timedelta(weeks=1)
    elif interval == "month":
        end = start + timedelta(days=31)
    else:
        raise ValueError(f"Unsupported interval: {interval}")
    return start, end


def process_source(source_type: SourceType, params: Dict[str, Any]) -> Dict[str, Any]:
    repo = get_repository()
    logger.info(f"ETL process started: source={source_type}, params={params}")

    # --- Предзагрузка кэшей ---
    logger.info("Preloading existing permanent ehr_ids...")
    existing_permanent: Set[int] = repo.get_all_permanent_ehr_ids()
    logger.info(f"Loaded {len(existing_permanent)} existing permanent ehr_ids")

    logger.info("Preloading latest changeable records...")
    all_ehr_for_change = list(existing_permanent) + [None]
    last_change: Dict[Optional[int], ChangeableUserProperties] = (
        repo.get_latest_changeable_for_ehrs(all_ehr_for_change)
    )
    logger.info(f"Loaded {len(last_change)} latest changeable records")

    processed = 0
    errors_count = 0
    last_successful_line: Optional[int] = None
    current_file_key: Optional[str] = None

    # Накопители для батчей
    pending_permanent: List[Dict[str, Any]] = []
    pending_changeable: List[Dict[str, Any]] = []
    batch_size = settings.etl.batch_size

    def flush():
        """Вставляет накопленные батчи в БД, используя только локальные кэши."""
        nonlocal pending_permanent, pending_changeable, existing_permanent, last_change

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
            from app.s3.client import S3Client

            s3 = S3Client()
            bucket = params["bucket"]
            prefix = params["prefix"]
            start_after_idx = int(params.get("start_after", 0))

            current_file_key = prefix

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

            for line_idx, line in enumerate(
                lines[start_after_idx:], start=start_after_idx
            ):
                try:
                    raw_record = json.loads(line.strip())
                except json.JSONDecodeError as e:
                    flush()
                    raise ProcessingInterrupted(
                        f"Невалидный JSON на строке {line_idx}",
                        last_successful_line=line_idx - 1,
                        failed_line=line_idx,
                        file_key=prefix,
                    )

                permanent, changeable, transform_errors = transform_single_record(
                    raw_record, source_type="amplitude"
                )

                if transform_errors:
                    log_bads(transform_errors)
                    errors_count += len(transform_errors)
                    error_msg = _format_transform_error(transform_errors)
                    flush()
                    raise ProcessingInterrupted(
                        error_msg,
                        last_successful_line=line_idx - 1,
                        failed_line=line_idx,
                        file_key=prefix,
                    )

                if permanent:
                    pending_permanent.append(permanent.model_dump())
                if changeable:
                    pending_changeable.append(changeable.model_dump())

                processed += 1
                last_successful_line = line_idx

                if (
                    len(pending_permanent) >= batch_size
                    or len(pending_changeable) >= batch_size
                ):
                    flush()

            flush()
            logger.info(f"Amplitude ETL finished, processed {processed} records")
        elif source_type == "tmp_table":
            start_date = params["start_date"]
            interval = params["interval"]
            start_dt, end_dt = _get_date_range(start_date, interval)

            logger.info(
                f"Selecting unmigrated rows from tmp_user_properties for period {start_dt} to {end_dt}"
            )
            rows = repo.select(
                table="tmp_user_properties",
                where={"migrated": False},
                where_conditions=[
                    ("event_time", ">=", start_dt),
                    ("event_time", "<", end_dt),
                ],
                order_by=["event_time"],
            )
            logger.info(f"Selected {len(rows)} unmigrated rows in period")

            batch_uuids: List[UUID] = []

            for row in rows:
                raw_record = dict(row)

                permanent, changeable, transform_errors = transform_single_record(
                    raw_record, source_type="tmp_table"
                )
                if transform_errors:
                    log_bads(transform_errors)
                    errors_count += len(transform_errors)
                    uuid_val = raw_record.get("uuid", "неизвестно")
                    error_msg = _format_transform_error(transform_errors)
                    full_msg = f"Ошибка трансформации для uuid={uuid_val}: {error_msg}"
                    flush()
                    if batch_uuids:
                        repo.update_migrated_batch(batch_uuids, migrated=True)
                    raise ProcessingInterrupted(full_msg)

                if permanent:
                    pending_permanent.append(permanent.model_dump())
                if changeable:
                    pending_changeable.append(changeable.model_dump())

                batch_uuids.append(raw_record["uuid"])
                processed += 1

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

            logger.info(f"Tmp_table ETL finished, processed {processed} records")

        return {
            "status": "completed",
            "processed": processed,
            "errors": errors_count,
            "last_successful_line": str(last_successful_line)
            if last_successful_line is not None
            else None,
        }

    except ProcessingInterrupted as pie:
        logger.error("Обработка прервана: %s", pie.message)
        response = {
            "status": "interrupted",
            "processed": processed,
            "errors": errors_count + 1,
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

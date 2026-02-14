import json

from typing import Dict, List, Optional, Set, Any
from fastapi import HTTPException

from app.etl.transformer import transform_single_record, SourceType
from app.db.schemas import (
    PermanentUserProperties,
    ChangeableUserProperties,
)
from app.db.repository import get_repository, DBRepository
from app.config.logger import get_logger

logger = get_logger(__name__)


def process_source(source_type: SourceType, params: Dict[str, Any]) -> Dict[str, Any]:
    repo = get_repository()


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

    existing_permanent: Set[int] = repo.get_all_permanent_ehr_ids()
    last_change: Dict[Optional[int], ChangeableUserProperties] = (
        repo.get_latest_changeable_for_ehrs(list(existing_permanent) + [None])
    )

    processed = 0
    errors_count = 0
    last_successful_line: Optional[int] = None
    current_file_key: Optional[str] = None

    try:
        if source_type == "amplitude":  # S3
            from app.s3.client import S3Client

            s3 = S3Client()
            bucket = params["bucket"]
            prefix = params["prefix"]
            start_after_idx = int(params.get("start_after", 0))

            current_file_key = prefix

            try:
                content = s3.get_object(bucket, prefix).decode("utf-8")
                lines = content.splitlines()
            except Exception as e:
                raise ProcessingInterrupted(
                    f"Не удалось прочитать файл S3: {str(e)}", file_key=prefix
                )

            if start_after_idx >= len(lines):
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
                    raise ProcessingInterrupted(
                        error_msg,
                        last_successful_line=line_idx - 1,
                        failed_line=line_idx,
                        file_key=prefix,
                    )

                _process_record(
                    repo, permanent, changeable, existing_permanent, last_change
                )

                processed += 1
                last_successful_line = line_idx

        elif source_type == "tmp_table":
            unmigrated_rows = repo.select(
                table="tmp_user_properties",
                where={"migrated": False},
                order_by=["event_time"],
            )

            for row in unmigrated_rows:
                raw_record = dict(row)

                permanent, changeable, transform_errors = transform_single_record(
                    raw_record, source_type="tmp_table"
                )
                logger.info("Data transformed")
                if transform_errors:
                    log_bads(transform_errors)
                    errors_count += len(transform_errors)
                    uuid_val = raw_record.get("uuid", "неизвестно")
                    error_msg = _format_transform_error(transform_errors)
                    full_msg = f"Ошибка трансформации для uuid={uuid_val}: {error_msg}"
                    raise ProcessingInterrupted(full_msg)

                _process_record(
                    repo, permanent, changeable, existing_permanent, last_change
                )

                try:
                    repo.update_migrated_tmp(raw_record["uuid"], migrated=True)
                except Exception as e:
                    raise ProcessingInterrupted(
                        f"Не удалось отметить migrated для uuid={raw_record['uuid']}: {str(e)}"
                    )

                processed += 1

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


def _process_record(
    repo: DBRepository,
    permanent: Optional[PermanentUserProperties],
    changeable: Optional[ChangeableUserProperties],
    existing_permanent: Set[int],
    last_change: Dict[Optional[int], ChangeableUserProperties],
):
    if permanent and permanent.ehr_id not in existing_permanent:
        try:
            logger.info("insert_permanent")
            repo.insert_permanent(permanent)
            existing_permanent.add(permanent.ehr_id)
            logger.info("inserted")
        except Exception as e:
            raise ProcessingInterrupted(
                f"Ошибка вставки permanent ehr_id={permanent.ehr_id}: {str(e)}"
            )

    if changeable:
        ehr_id = changeable.ehr_id
        old = last_change.get(ehr_id)

        if compare_changeable(old, changeable):
            try:
                logger.info("insert_changeable")
                repo.insert_changeable(changeable)
                last_change[ehr_id] = changeable
                logger.info("inserted")
            except Exception as e:
                raise ProcessingInterrupted(
                    f"Ошибка upsert changeable ehr_id={ehr_id}: {str(e)}"
                )

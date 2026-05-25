import io
import json
import zipfile
from typing import Dict, Any, List, Tuple, Optional, Set
from collections import defaultdict

from app.config.logger import get_logger
from app.s3.client import S3Client
from app.db.repository import get_repository
from app.etl.config_models import ETLConfig, TableConfig, FieldMapping, SourceS3Config
from app.etl.utils import (
    extract_value_by_path,
    apply_value_map,
    convert_type,
    natural_sort_key,
    find_unknown_keys,
)

logger = get_logger(__name__)


class ProcessingInterrupted(Exception):
    def __init__(
        self,
        message,
        failed_file=None,
        failed_line=None,
        last_successful_file=None,
        last_successful_line=None,
        error_details=None,
    ):
        self.message = message
        self.failed_file = failed_file
        self.failed_line = failed_line
        self.last_successful_file = last_successful_file
        self.last_successful_line = last_successful_line
        self.error_details = error_details
        super().__init__(message)


def _transform_record(
    record: Dict, table_cfg: TableConfig, all_known_paths: Set[str]
) -> Tuple[Optional[Dict], List[str]]:
    """Применяет маппинг к одной сырой записи. Возвращает (data, errors)."""
    data = {}
    errors = []

    for field in table_cfg.fields:
        # Пропускаем игнорируемые поля
        if getattr(field, "ignore", False):
            continue

        value = None
        for src in field.sources:
            val = extract_value_by_path(record, src)
            if val is not None:
                value = val
                break
        if value is None and field.default is not None:
            value = field.default

        # Применяем null_values и value_map
        value = apply_value_map(value, field.value_map or {}, field.null_values or [])

        if value is None and field.required:
            errors.append(
                f"Required field {field.target} missing (sources: {field.sources})"
            )
            continue

        if value is not None:
            try:
                value = convert_type(value, field.type, field.format)
            except Exception as e:
                errors.append(f"Field {field.target}: {e}")
                continue

        data[field.target] = value

    return data, errors


def _insert_or_update_batch(
    repo, table_name: str, rows: List[Dict], table_cfg: TableConfig
) -> int:
    """Вставляет или обновляет батч строк. Возвращает количество обработанных строк."""
    if not rows:
        return 0

    # Если есть первичный ключ и требуется обновление при конфликте
    if table_cfg.primary_key and table_cfg.update_strategy == "on_change":
        conflict_target = ", ".join(table_cfg.primary_key)
        # Формируем SET clause: обновляем все колонки, кроме первичного ключа
        set_clause_parts = []
        for col in rows[0].keys():
            if col not in table_cfg.primary_key:
                set_clause_parts.append(f"{col} = EXCLUDED.{col}")
        set_clause = ", ".join(set_clause_parts)
        inserted_ids, _ = repo.upsert_batch(
            table=table_name,
            rows=rows,
            conflict_target=conflict_target,
            set_clause=set_clause,
            returning_column=table_cfg.primary_key[0],
        )
        return len(inserted_ids)

    # Если on_conflict = "DO NOTHING" или "DO UPDATE" (без on_change)
    if table_cfg.on_conflict and table_cfg.primary_key:
        conflict_target = ", ".join(table_cfg.primary_key)
        inserted_ids, _ = repo.insert_batch(
            table=table_name,
            rows=rows,
            on_conflict=table_cfg.on_conflict,
            conflict_target=f"({conflict_target})",
            returning_column=table_cfg.primary_key[0],
        )
        return len(inserted_ids)

    # Обычная вставка (без конфликта)
    inserted_ids, _ = repo.insert_batch(table=table_name, rows=rows)
    return len(inserted_ids)


def deduplicate_buffer(table_cfg: TableConfig, buffer: List[Dict]) -> List[Dict]:
    """
    Удаляет дубликаты по первичному ключу, оставляя последнюю запись.
    Подходит для таблиц с ON CONFLICT DO UPDATE.
    """
    if not table_cfg.primary_key or table_cfg.on_conflict != "DO UPDATE":
        return buffer

    pk_fields = table_cfg.primary_key
    last_by_key = {}
    for row in buffer:
        key = tuple(row.get(field) for field in pk_fields)
        current_time = row.get("event_time")
        if key not in last_by_key:
            last_by_key[key] = row
        else:
            existing_time = last_by_key[key].get("event_time")
            if current_time and existing_time and current_time > existing_time:
                last_by_key[key] = row
            elif not existing_time:
                last_by_key[key] = row
    return list(last_by_key.values())


def flush_table_buffer(
    table_cfg: TableConfig,
    buffers: Dict[str, List[Dict]],
    repo,
    stats: Dict,
    force: bool = False,
) -> None:
    """Сбросить буфер таблицы в БД (с дедупликацией при необходимости)."""
    table_name = table_cfg.name
    buffer = buffers.get(table_name, [])
    if not buffer:
        return
    if not force and len(buffer) < table_cfg.batch_size:
        return

    # Дедуплицируем только если ON CONFLICT DO UPDATE
    if table_cfg.primary_key and table_cfg.on_conflict == "DO UPDATE":
        buffer = deduplicate_buffer(table_cfg, buffer)
        if not buffer:
            buffers[table_name] = []
            return

    inserted = _insert_or_update_batch(repo, table_name, buffer, table_cfg)
    stats["batches"][table_name] += inserted
    buffers[table_name] = []


def _process_s3_files(
    s3_client: S3Client,
    source_cfg: SourceS3Config,
    start_after_file: Optional[str],
    start_after_line: int,
    tables_config: List[TableConfig],
) -> Dict[str, Any]:
    """Основной цикл по файлам S3 с батчевой вставкой."""
    bucket = source_cfg.bucket
    prefix = source_cfg.prefix
    sort_cfg = source_cfg.sort
    pattern = source_cfg.file_pattern.replace("*", "")

    objects = s3_client.list_objects(prefix=prefix)
    files = [obj for obj in objects if obj["Key"].endswith(pattern)]
    if not files:
        raise ValueError(f"No files found in {bucket}/{prefix}")

    if sort_cfg.by == "key":
        key_func = natural_sort_key if sort_cfg.natural else lambda x: x["Key"]
        files.sort(key=lambda x: key_func(x["Key"]), reverse=(sort_cfg.order == "desc"))
    else:
        files.sort(key=lambda x: x["LastModified"], reverse=(sort_cfg.order == "desc"))

    start_idx = 0
    if start_after_file:
        for i, obj in enumerate(files):
            if obj["Key"] == start_after_file:
                start_idx = i
                break

    stats = {"files_processed": 0, "lines_processed": 0, "batches": defaultdict(int)}
    last_successful_file = None
    last_successful_line = 0

    # Собираем известные пути и пути JSONB-полей
    all_known_paths = set()
    jsonb_source_prefixes = set()
    for table in tables_config:
        for field in table.fields:
            all_known_paths.update(field.sources)
            if field.type == "json":
                for src in field.sources:
                    jsonb_source_prefixes.add(src)

    repo = get_repository()
    # Буферы для каждой таблицы
    buffers = {table.name: [] for table in tables_config}

    for idx in range(start_idx, len(files)):
        file_obj = files[idx]
        file_key = file_obj["Key"]
        logger.info(f"Processing file {file_key} ({idx + 1}/{len(files)})")
        try:
            zip_bytes = s3_client.get_object(file_key)
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                ndjson_files = [
                    name for name in zf.namelist() if name.endswith(".ndjson")
                ]
                if not ndjson_files:
                    logger.warning(f"No NDJSON files in {file_key}")
                    continue
                ndjson_content = zf.read(ndjson_files[0]).decode("utf-8")
                lines = ndjson_content.splitlines()
                start_line = start_after_line if idx == start_idx else 0
                for line_num, line in enumerate(
                    lines[start_line:], start=start_line + 1
                ):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw_record = json.loads(line)
                    except json.JSONDecodeError as e:
                        raise ProcessingInterrupted(
                            f"Invalid JSON in {file_key}:{line_num}",
                            failed_file=file_key,
                            failed_line=line_num,
                            last_successful_file=file_key,
                            last_successful_line=line_num - 1,
                        )

                    # Проверка неизвестных полей (игнорируем JSONB)
                    unknown = find_unknown_keys(
                        raw_record, all_known_paths, jsonb_source_prefixes
                    )
                    if unknown:
                        raise ProcessingInterrupted(
                            f"Unknown keys in record: {', '.join(sorted(unknown))}",
                            failed_file=file_key,
                            failed_line=line_num,
                            last_successful_file=file_key,
                            last_successful_line=line_num - 1,
                        )

                    # Трансформируем и накапливаем в буферах
                    for table_cfg in tables_config:
                        data, errors = _transform_record(
                            raw_record, table_cfg, all_known_paths
                        )
                        if errors:
                            raise ProcessingInterrupted(
                                f"Transformation errors: {'; '.join(errors)}",
                                failed_file=file_key,
                                failed_line=line_num,
                                last_successful_file=file_key,
                                last_successful_line=line_num - 1,
                            )
                        if data:
                            buffers[table_cfg.name].append(data)
                            # Сбрасываем буфер, если достигнут batch_size
                            if len(buffers[table_cfg.name]) >= table_cfg.batch_size:
                                flush_table_buffer(
                                    table_cfg, buffers, repo, stats, force=False
                                )

                    stats["lines_processed"] += 1
                    last_successful_line = line_num

                # После окончания файла – принудительный сброс всех буферов
                for table_cfg in tables_config:
                    flush_table_buffer(table_cfg, buffers, repo, stats, force=True)

                last_successful_file = file_key
                start_after_line = 0
        except ProcessingInterrupted:
            raise
        except Exception as e:
            raise ProcessingInterrupted(
                str(e),
                failed_file=file_key,
                failed_line=0,
                last_successful_file=last_successful_file,
                last_successful_line=last_successful_line,
            )
        stats["files_processed"] += 1

    # Финальный сброс (на случай, если что-то осталось)
    for table_cfg in tables_config:
        flush_table_buffer(table_cfg, buffers, repo, stats, force=True)

    return stats


def process_universal_etl(
    etl_config: ETLConfig, source_params: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Главная точка входа для универсального ETL.

    Args:
        etl_config: Валидированный объект конфигурации (из YAML)
        source_params: Опциональные параметры для возобновления:
            - start_after_file: str (S3 ключ файла)
            - start_after_line: int (номер строки, 0-based)
    """
    source_type = etl_config.source.type
    if source_type != "s3":
        raise ValueError(f"Only s3 source supported currently, got {source_type}")

    s3_client = S3Client()
    start_after_file = None
    start_after_line = 0
    if source_params:
        start_after_file = source_params.get("start_after_file")
        start_after_line = source_params.get("start_after_line", 0)

    stats = _process_s3_files(
        s3_client=s3_client,
        source_cfg=etl_config.source,
        start_after_file=start_after_file,
        start_after_line=start_after_line,
        tables_config=etl_config.tables,
    )
    return stats

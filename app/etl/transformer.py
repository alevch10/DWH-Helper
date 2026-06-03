import asyncio
import csv
import io
import json
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from app.appmetrica.client import AppMetricaClient
from app.config.logger import get_logger
from app.config.settings import settings
from app.db.repository import get_repository
from app.etl.config_models import (
    ETLConfig,
    SourceAppMetricaConfig,
    SourceS3Config,
    SourceYandexMetrikaConfig,
    TableConfig,
)
from app.etl.utils import (
    apply_value_map,
    convert_type,
    extract_value_by_path,
    find_unknown_keys,
    natural_sort_key,
)
from app.s3.client import S3Client
from app.yandex_metrika.schemas import MetrikaHitRow
from app.yandex_metrika.services import stream_metrika_lines

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
        failed_date=None,
        last_successful_date=None,
    ):
        self.message = message
        self.failed_file = failed_file
        self.failed_line = failed_line
        self.last_successful_file = last_successful_file
        self.last_successful_line = last_successful_line
        self.error_details = error_details
        self.failed_date = failed_date
        self.last_successful_date = last_successful_date
        super().__init__(message)


def _transform_record(
    record: Dict, table_cfg: TableConfig, all_known_paths: Set[str]
) -> Tuple[Optional[Dict], List[str]]:
    """Применяет маппинг к одной сырой записи. Возвращает (data, errors)."""
    data = {}
    errors = []

    for field in table_cfg.fields:
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
    repo, table_name: str, rows: List[Dict], table_cfg: TableConfig, conn=None
) -> int:
    """Вставляет или обновляет батч строк. Возвращает количество обработанных строк."""
    if not rows:
        return 0

    if table_cfg.primary_key and table_cfg.update_strategy == "on_change":
        conflict_target = ", ".join(table_cfg.primary_key)
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
            conn=conn,
        )
        return len(inserted_ids)

    if table_cfg.on_conflict and table_cfg.primary_key:
        conflict_target = ", ".join(table_cfg.primary_key)
        inserted_ids, _ = repo.insert_batch(
            table=table_name,
            rows=rows,
            on_conflict=table_cfg.on_conflict,
            conflict_target=f"({conflict_target})",
            returning_column=table_cfg.primary_key[0],
            conn=conn,
        )
        return len(inserted_ids)

    inserted_ids, _ = repo.insert_batch(table=table_name, rows=rows, conn=conn)
    return len(inserted_ids)


def deduplicate_buffer(table_cfg: TableConfig, buffer: List[Dict]) -> List[Dict]:
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
    conn=None,
) -> None:
    table_name = table_cfg.name
    buffer = buffers.get(table_name, [])
    if not buffer:
        return
    if not force and len(buffer) < table_cfg.batch_size:
        return

    if table_cfg.primary_key and table_cfg.on_conflict == "DO UPDATE":
        buffer = deduplicate_buffer(table_cfg, buffer)
        if not buffer:
            buffers[table_name] = []
            return

    inserted = _insert_or_update_batch(repo, table_name, buffer, table_cfg, conn=conn)
    stats["batches"][table_name] += inserted
    buffers[table_name] = []


def _parse_appmetrica_csv(csv_text: str) -> List[Dict[str, str]]:
    """Разбирает CSV-строку от AppMetrica, очищая заголовки от BOM и пробелов."""
    if not csv_text.strip():
        return []
    if csv_text.startswith('\ufeff'):
        csv_text = csv_text[1:]
    reader = csv.reader(io.StringIO(csv_text))
    raw_headers = next(reader, [])
    if not raw_headers:
        return []
    headers = [h.strip().strip('"').lstrip('\ufeff') for h in raw_headers]
    logger.debug("AppMetrica CSV headers after cleaning: %s", headers)
    rows = []
    for row in reader:
        if not row:
            continue
        if len(row) < len(headers):
            row.extend([''] * (len(headers) - len(row)))
        record = {}
        for i, h in enumerate(headers):
            value = row[i].strip().strip('"') if i < len(row) else ''
            record[h] = value
        rows.append(record)
    return rows


def _process_s3_files(
    s3_client: S3Client,
    source_cfg: SourceS3Config,
    start_after_file: Optional[str],
    start_after_line: int,
    tables_config: List[TableConfig],
) -> Dict[str, Any]:
    """Синхронная обработка S3-файлов (без изменений)."""
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

    all_known_paths = set()
    jsonb_source_prefixes = set()
    for table in tables_config:
        for field in table.fields:
            all_known_paths.update(field.sources)
            if field.type == "json":
                for src in field.sources:
                    jsonb_source_prefixes.add(src)

    repo = get_repository()
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
                            if len(buffers[table_cfg.name]) >= table_cfg.batch_size:
                                flush_table_buffer(
                                    table_cfg, buffers, repo, stats, force=False
                                )

                    stats["lines_processed"] += 1
                    last_successful_line = line_num

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

    for table_cfg in tables_config:
        flush_table_buffer(table_cfg, buffers, repo, stats, force=True)

    return stats


async def _process_yandex_metrika_async(
    source_cfg: SourceYandexMetrikaConfig,
    tables_config: List[TableConfig],
    token: str,
    start_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Асинхронная обработка данных из Яндекс.Метрики чанками по chunk_days дней."""
    fields_list = source_cfg.fields
    if not fields_list:
        fields_list = settings.yandexmetrica.default_fields.split(",")
    fields_str = ",".join(fields_list)

    date_from = source_cfg.date_from
    date_to = source_cfg.date_to
    if start_date:
        date_from = max(date_from, datetime.strptime(start_date, "%Y-%m-%d").date())

    chunk_days = source_cfg.chunk_days
    current_start = date_from

    repo = get_repository()
    stats = {"files_processed": 0, "lines_processed": 0, "batches": defaultdict(int)}

    # Нормализуем запрошенные поля для проверки
    normalized_requested = set()
    for f in fields_list:
        f = f.replace("ym:pv:", "")
        f = f.replace("from", "from_")
        normalized_requested.add(f)

    all_known_paths = set()
    jsonb_source_prefixes = set()
    for table in tables_config:
        for field in table.fields:
            all_known_paths.update(field.sources)
            if field.type == "json":
                for src in field.sources:
                    jsonb_source_prefixes.add(src)

    while current_start <= date_to:
        chunk_end = min(
            current_start + timedelta(days=chunk_days) - timedelta(days=1), date_to
        )
        date1 = current_start.strftime("%Y-%m-%d")
        date2 = chunk_end.strftime("%Y-%m-%d")
        logger.info(f"Processing chunk {date1} -> {date2}")

        conn = repo.get_raw_connection()
        buffers = {table.name: [] for table in tables_config}
        chunk_lines = 0

        try:
            async for row_dict in stream_metrika_lines(
                token,
                source_cfg.counter_id,
                date1,
                date2,
                source_cfg.source,
                fields_str,
            ):
                logger.debug("Received row: %s", str(row_dict)[:200])
                chunk_lines += 1

                unknown = set(row_dict.keys()) - normalized_requested
                if unknown:
                    raise ProcessingInterrupted(
                        f"Unexpected fields in response: {', '.join(sorted(unknown))}",
                        failed_date=current_start.strftime("%Y-%m-%d"),
                        last_successful_date=(current_start - timedelta(days=1)).strftime(
                            "%Y-%m-%d"
                        ) if current_start > date_from else None,
                    )

                try:
                    hit = MetrikaHitRow.model_validate(row_dict)
                    record = hit.model_dump(by_alias=True)
                except Exception as e:
                    raise ProcessingInterrupted(
                        f"Validation error: {str(e)}",
                        failed_date=current_start.strftime("%Y-%m-%d"),
                        last_successful_date=(current_start - timedelta(days=1)).strftime(
                            "%Y-%m-%d"
                        ) if current_start > date_from else None,
                    )

                for table_cfg in tables_config:
                    data, errors = _transform_record(record, table_cfg, all_known_paths)
                    if errors:
                        raise ProcessingInterrupted(
                            f"Transformation errors: {'; '.join(errors)}",
                            failed_date=current_start.strftime("%Y-%m-%d"),
                            last_successful_date=(current_start - timedelta(days=1)).strftime(
                                "%Y-%m-%d"
                            ) if current_start > date_from else None,
                        )
                    if data:
                        buffers[table_cfg.name].append(data)
                        if len(buffers[table_cfg.name]) >= table_cfg.batch_size:
                            flush_table_buffer(
                                table_cfg, buffers, repo, stats, force=False, conn=conn
                            )

                stats["lines_processed"] += 1

            for table_cfg in tables_config:
                flush_table_buffer(table_cfg, buffers, repo, stats, force=True, conn=conn)

            repo.commit(conn)
            stats["files_processed"] += 1
            logger.info(
                f"Chunk {date1} -> {date2} committed, lines: {chunk_lines}"
            )

        except ProcessingInterrupted:
            repo.rollback(conn)
            raise
        except Exception as e:
            repo.rollback(conn)
            raise ProcessingInterrupted(
                str(e),
                failed_date=current_start.strftime("%Y-%m-%d"),
                last_successful_date=(current_start - timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                ) if current_start > date_from else None,
            )

        current_start = chunk_end + timedelta(days=1)

    return stats


async def _process_appmetrica_async(
    source_cfg: SourceAppMetricaConfig,
    tables_config: List[TableConfig],
    token: str,
    start_date: Optional[str] = None,
) -> Dict[str, Any]:
    """Асинхронная обработка данных AppMetrica чанками."""
    api_key = token

    app_id = source_cfg.application_id or settings.appmetrica.application_id
    if not app_id:
        raise ValueError("application_id is required")

    default_fields_str = (
        "app_build_number,profile_id,os_name,os_version,device_manufacturer,device_model,device_type,"
        "device_locale,device_ipv6,app_version_name,event_name,event_json,connection_type,operator_name,"
        "country_iso_code,city,appmetrica_device_id,installation_id,session_id,event_datetime"
    )
    fields_list = source_cfg.fields
    if not fields_list:
        fields_list = default_fields_str.split(",")
    fields_str = ",".join(fields_list)

    date_from = source_cfg.date_since
    date_to = source_cfg.date_until
    if start_date:
        date_from = max(date_from, datetime.strptime(start_date, "%Y-%m-%d").date())

    chunk_days = source_cfg.chunk_days
    current_start = date_from

    stats = {"files_processed": 0, "lines_processed": 0, "batches": defaultdict(int)}
    repo = get_repository()

    normalized_requested = set(fields_list)

    all_known_paths = set()
    jsonb_source_prefixes = set()
    for table in tables_config:
        for field in table.fields:
            all_known_paths.update(field.sources)
            if field.type == "json":
                for src in field.sources:
                    jsonb_source_prefixes.add(src)

    client = AppMetricaClient()

    while current_start <= date_to:
        chunk_end = min(
            current_start + timedelta(days=chunk_days) - timedelta(days=1), date_to
        )
        date_since_str = current_start.strftime("%Y-%m-%d 00:00:00")
        date_until_str = chunk_end.strftime("%Y-%m-%d 23:59:59")
        logger.info(f"Processing AppMetrica chunk: {date_since_str} -> {date_until_str}")

        try:
            fetch_result = await client.fetch_export(
                application_id=app_id,
                skip_unavailable_shards=source_cfg.skip_unavailable_shards,
                date_since=date_since_str,
                date_until=date_until_str,
                date_dimension=source_cfg.date_dimension,
                use_utf8_bom=source_cfg.use_utf8_bom,
                fields=fields_str,
                export_format=source_cfg.export_format,
                api_key=api_key,
                poll_timeout=86400 * 30,  # практически бесконечно
            )
        except Exception as e:
            raise ProcessingInterrupted(
                f"AppMetrica export failed: {str(e)}",
                failed_date=current_start.strftime("%Y-%m-%d"),
                last_successful_date=(current_start - timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                ) if current_start > date_from else None,
            )

        if fetch_result["status"] != "ready":
            raise ProcessingInterrupted(
                f"AppMetrica export not ready: {fetch_result.get('detail', 'unknown')}",
                failed_date=current_start.strftime("%Y-%m-%d"),
                last_successful_date=(current_start - timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                ) if current_start > date_from else None,
            )

        raw_data = fetch_result["result"]
        if source_cfg.export_format == "json":
            events = raw_data.get("data", [])
        else:
            events = _parse_appmetrica_csv(raw_data)

        if not events:
            logger.info(f"No events in chunk {date_since_str} - {date_until_str}")
            current_start = chunk_end + timedelta(days=1)
            continue

        logger.debug("First record sample keys: %s", list(events[0].keys()))
        logger.debug("Normalized requested fields: %s", normalized_requested)

        conn = repo.get_raw_connection()
        buffers = {table.name: [] for table in tables_config}
        chunk_lines = 0

        try:
            for record in events:
                logger.debug("Record keys: %s", list(record.keys()))

                unknown = set(record.keys()) - normalized_requested
                if unknown:
                    raise ProcessingInterrupted(
                        f"Unexpected fields in record: {', '.join(sorted(unknown))}",
                        failed_date=current_start.strftime("%Y-%m-%d"),
                        last_successful_date=(current_start - timedelta(days=1)).strftime(
                            "%Y-%m-%d"
                        ) if current_start > date_from else None,
                    )

                for table_cfg in tables_config:
                    data, errors = _transform_record(record, table_cfg, all_known_paths)
                    if errors:
                        raise ProcessingInterrupted(
                            f"Transformation errors: {'; '.join(errors)}",
                            failed_date=current_start.strftime("%Y-%m-%d"),
                            last_successful_date=(current_start - timedelta(days=1)).strftime(
                                "%Y-%m-%d"
                            ) if current_start > date_from else None,
                        )
                    if data:
                        buffers[table_cfg.name].append(data)
                        if len(buffers[table_cfg.name]) >= table_cfg.batch_size:
                            flush_table_buffer(
                                table_cfg, buffers, repo, stats, force=False, conn=conn
                            )

                chunk_lines += 1
                stats["lines_processed"] += 1

            for table_cfg in tables_config:
                flush_table_buffer(table_cfg, buffers, repo, stats, force=True, conn=conn)

            repo.commit(conn)
            stats["files_processed"] += 1
            logger.info(
                f"Chunk {date_since_str} - {date_until_str} committed, lines: {chunk_lines}"
            )

        except ProcessingInterrupted:
            repo.rollback(conn)
            raise
        except Exception as e:
            repo.rollback(conn)
            raise ProcessingInterrupted(
                str(e),
                failed_date=current_start.strftime("%Y-%m-%d"),
                last_successful_date=(current_start - timedelta(days=1)).strftime(
                    "%Y-%m-%d"
                ) if current_start > date_from else None,
            )

        current_start = chunk_end + timedelta(days=1)

    return stats


async def process_universal_etl(
    etl_config: ETLConfig,
    source_params: Optional[Dict] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Главная точка входа для универсального ETL (асинхронная)."""
    source = etl_config.source
    if source.type == "s3":
        s3_client = S3Client()
        start_after_file = None
        start_after_line = 0
        if source_params:
            start_after_file = source_params.get("start_after_file")
            start_after_line = source_params.get("start_after_line", 0)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            _process_s3_files,
            s3_client,
            source,
            start_after_file,
            start_after_line,
            etl_config.tables,
        )
    elif source.type == "yandex_metrika":
        if not token:
            raise ValueError("Token is required for yandex_metrika source")
        start_date = None
        if source_params:
            start_date = source_params.get("start_date")
        return await _process_yandex_metrika_async(
            source, etl_config.tables, token, start_date
        )
    elif source.type == "appmetrica":
        if not token:
            raise ValueError("Token is required for appmetrica source")
        start_date = None
        if source_params:
            start_date = source_params.get("start_date")
        return await _process_appmetrica_async(
            source, etl_config.tables, token, start_date
        )
    else:
        raise ValueError(f"Unsupported source type: {source.type}")
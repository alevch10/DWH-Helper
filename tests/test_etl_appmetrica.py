import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.etl.config_models import ETLConfig
from app.etl.transformer import (
    process_universal_etl,
    ProcessingInterrupted,
    _parse_appmetrica_csv,
)
from app.db.repository import DBRepository


# ----------------------------------------------------------------------
# Фикстуры
# ----------------------------------------------------------------------
@pytest.fixture
def appmetrica_config():
    """Один день, чтобы проще тестировать."""
    return ETLConfig.model_validate(
        {
            "source": {
                "type": "appmetrica",
                "application_id": 123,
                "date_since": "2026-06-01",
                "date_until": "2026-06-01",  # один день
                "export_format": "csv",
                "fields": [
                    "event_datetime",
                    "event_json",
                    "profile_id",
                    "event_name",
                ],
                "chunk_days": 2,
            },
            "tables": [
                {
                    "name": "appmetrica.events",
                    "batch_size": 2,
                    "fields": [
                        {
                            "target": "event_time",
                            "sources": ["event_datetime"],
                            "type": "datetime",
                        },
                        {
                            "target": "event_json",
                            "sources": ["event_json"],
                            "type": "json",
                        },
                    ],
                },
                {
                    "name": "appmetrica.event_params",
                    "batch_size": 2,
                    "fields": [
                        {
                            "target": "profile_id",
                            "sources": ["profile_id"],
                            "type": "string",
                        },
                        {
                            "target": "event_name",
                            "sources": ["event_name"],
                            "type": "string",
                        },
                    ],
                },
            ],
        }
    )


@pytest.fixture
def mock_repo():
    """Мок DBRepository."""
    repo = MagicMock(spec=DBRepository)
    repo.insert_batch.return_value = ([], 1)
    repo.upsert_batch.return_value = ([], 1)
    repo.get_raw_connection.return_value = MagicMock()
    repo.commit = MagicMock()
    repo.rollback = MagicMock()
    return repo


def make_csv_response(rows, fieldnames=None, include_bom=False):
    """
    Генерирует CSV-строку с заданными строками.
    :param rows: список словарей
    :param fieldnames: список заголовков (по умолчанию стандартный набор)
    :param include_bom: добавлять ли BOM
    """
    import csv, io

    if fieldnames is None:
        fieldnames = ["event_datetime", "event_json", "profile_id", "event_name"]
    output = io.StringIO()
    if include_bom:
        output.write("\ufeff")
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        # Дополняем недостающие поля пустыми строками
        full_row = {k: row.get(k, "") for k in fieldnames}
        writer.writerow(full_row)
    return output.getvalue()


# ----------------------------------------------------------------------
# Тесты парсера CSV
# ----------------------------------------------------------------------
def test_parse_csv_clean():
    csv_text = "event_datetime, event_json , profile_id\n2026-06-01 12:00:00,{} ,123\n"
    rows = _parse_appmetrica_csv(csv_text)
    assert len(rows) == 1
    assert rows[0]["event_datetime"] == "2026-06-01 12:00:00"
    assert rows[0]["event_json"] == "{}"
    assert rows[0]["profile_id"] == "123"


def test_parse_csv_with_bom():
    csv_text = "\ufeffevent_datetime,event_json\n2026-06-01 12:00:00,{}\n"
    rows = _parse_appmetrica_csv(csv_text)
    assert len(rows) == 1
    assert "event_datetime" in rows[0]


# ----------------------------------------------------------------------
# Интеграционные тесты
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_appmetrica_success(appmetrica_config, mock_repo):
    """Успешная обработка одного дня с двумя строками."""
    csv_data = make_csv_response(
        [
            {
                "event_datetime": "2026-06-01 12:00:00",
                "event_json": '{"a":1}',
                "profile_id": "1",
                "event_name": "Click",
            },
            {
                "event_datetime": "2026-06-01 12:01:00",
                "event_json": "",
                "profile_id": "2",
                "event_name": "View",
            },
        ]
    )
    mock_client = AsyncMock()
    mock_client.fetch_export.return_value = {"status": "ready", "result": csv_data}

    with patch("app.etl.transformer.AppMetricaClient", return_value=mock_client):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            stats = await process_universal_etl(
                etl_config=appmetrica_config,
                token="fake_token",
            )

    assert stats["lines_processed"] == 2
    assert stats["files_processed"] == 1
    mock_repo.commit.assert_called_once()
    mock_repo.rollback.assert_not_called()


@pytest.mark.asyncio
async def test_appmetrica_unknown_field(appmetrica_config, mock_repo):
    """Поле extra_field в CSV (присутствует в заголовках) вызывает ошибку."""
    # Создаём CSV с заголовками, включающими extra_field
    csv_data = make_csv_response(
        [
            {
                "event_datetime": "2026-06-01 12:00:00",
                "event_json": "{}",
                "profile_id": "1",
                "event_name": "X",
                "extra_field": "oops",
            },
        ],
        fieldnames=[
            "event_datetime",
            "event_json",
            "profile_id",
            "event_name",
            "extra_field",
        ],
    )
    mock_client = AsyncMock()
    mock_client.fetch_export.return_value = {"status": "ready", "result": csv_data}

    with patch("app.etl.transformer.AppMetricaClient", return_value=mock_client):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            with pytest.raises(ProcessingInterrupted) as exc:
                await process_universal_etl(
                    etl_config=appmetrica_config,
                    token="fake_token",
                )
    assert "Unexpected fields" in str(exc.value)
    mock_repo.rollback.assert_called()


@pytest.mark.asyncio
async def test_appmetrica_resume_from_start_date(appmetrica_config, mock_repo):
    """Проверка, что start_date пропускает дни (в данном случае этот день должен быть обработан)."""
    csv_data = make_csv_response(
        [
            {
                "event_datetime": "2026-06-01 12:00:00",
                "event_json": "{}",
                "profile_id": "1",
                "event_name": "Y",
            },
        ]
    )
    mock_client = AsyncMock()
    mock_client.fetch_export.return_value = {"status": "ready", "result": csv_data}

    with patch("app.etl.transformer.AppMetricaClient", return_value=mock_client):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            stats = await process_universal_etl(
                etl_config=appmetrica_config,
                token="fake_token",
                source_params={"start_date": "2026-06-01"},
            )
    assert stats["files_processed"] == 1
    assert stats["lines_processed"] == 1


@pytest.mark.asyncio
async def test_appmetrica_db_error_triggers_rollback(appmetrica_config, mock_repo):
    """Ошибка БД вызывает откат транзакции."""
    csv_data = make_csv_response(
        [
            {
                "event_datetime": "2026-06-01 12:00:00",
                "event_json": "{}",
                "profile_id": "1",
                "event_name": "Z",
            },
        ]
    )
    mock_client = AsyncMock()
    mock_client.fetch_export.return_value = {"status": "ready", "result": csv_data}
    mock_repo.insert_batch.side_effect = Exception("DB error")

    with patch("app.etl.transformer.AppMetricaClient", return_value=mock_client):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            with pytest.raises(ProcessingInterrupted) as exc:
                await process_universal_etl(
                    etl_config=appmetrica_config,
                    token="fake_token",
                )
    assert "DB error" in str(exc.value)
    mock_repo.rollback.assert_called()
    mock_repo.commit.assert_not_called()

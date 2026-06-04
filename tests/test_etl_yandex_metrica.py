import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

from app.etl.config_models import ETLConfig
from app.etl.transformer import (
    process_universal_etl,
    ProcessingInterrupted,
)
from app.db.repository import DBRepository


# ----------------------------------------------------------------------
# Фикстуры
# ----------------------------------------------------------------------
@pytest.fixture
def valid_yandex_metrika_config():
    """Минимальная валидная конфигурация для источника Яндекс.Метрики."""
    return ETLConfig.model_validate(
        {
            "source": {
                "type": "yandex_metrika",
                "counter_id": 123,
                "date_from": "2026-06-01",
                "date_to": "2026-06-02",
                "source": "hits",
                "fields": [
                    "ym:pv:watchID",
                    "ym:pv:clientID",
                    "ym:pv:dateTime",
                    "ym:pv:params",
                ],
                "chunk_days": 1,
            },
            "tables": [
                {
                    "name": "yandex_metrika.events",
                    "batch_size": 2,
                    "fields": [
                        {
                            "target": "watch_id",
                            "sources": ["watchID"],
                            "type": "integer",
                        },
                        {
                            "target": "client_id",
                            "sources": ["clientID"],
                            "type": "integer",
                        },
                        {
                            "target": "date_time",
                            "sources": ["dateTime"],
                            "type": "datetime",
                        },
                    ],
                },
                {
                    "name": "yandex_metrika.event_params",
                    "batch_size": 2,
                    "fields": [
                        {
                            "target": "watch_id",
                            "sources": ["watchID"],
                            "type": "integer",
                        },
                        {"target": "params", "sources": ["params"], "type": "string"},
                    ],
                },
            ],
        }
    )


@pytest.fixture
def mock_repo():
    """Мок DBRepository."""
    repo = MagicMock(spec=DBRepository)
    repo.insert_batch = MagicMock(return_value=([], 1))
    repo.upsert_batch = MagicMock(return_value=([], 1))
    repo.get_raw_connection = MagicMock(return_value=MagicMock())
    repo.commit = MagicMock()
    repo.rollback = MagicMock()
    return repo


def make_valid_row(
    watchID="1", clientID="100", dateTime="2026-06-01T12:00:00", params=None
):
    """
    Создать словарь, который вернул бы генератор stream_metrika_lines
    для запрошенных полей (watchID, clientID, dateTime, params).
    """
    return {
        "watchID": watchID,
        "clientID": clientID,
        "dateTime": dateTime,
        "params": params or "",
    }


# ----------------------------------------------------------------------
# Тесты на валидацию конфигурации
# ----------------------------------------------------------------------
def test_config_validation(valid_yandex_metrika_config):
    assert valid_yandex_metrika_config.source.type == "yandex_metrika"
    assert valid_yandex_metrika_config.source.date_from == date(2026, 6, 1)
    assert len(valid_yandex_metrika_config.tables) == 2


def test_config_default_fields(monkeypatch):
    """Проверка, что если fields не указан, берутся из settings."""
    monkeypatch.setattr(
        "app.etl.transformer.settings.yandexmetrica.default_fields",
        "ym:pv:watchID,ym:pv:clientID",
    )
    config = ETLConfig.model_validate(
        {
            "source": {
                "type": "yandex_metrika",
                "counter_id": 1,
                "date_from": "2026-01-01",
                "date_to": "2026-01-01",
                "source": "hits",
            },
            "tables": [],
        }
    )
    assert config.source.fields is None


# ----------------------------------------------------------------------
# Тесты асинхронного генератора stream_metrika_lines
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stream_metrika_lines():
    """Проверяем, что генератор возвращает нормализованные словари."""
    from app.yandex_metrika.services import stream_metrika_lines

    with patch("app.yandex_metrika.services.MetrikaClient") as MockClient:
        mock_client = AsyncMock()
        MockClient.return_value = mock_client

        # Мок-ответы API
        mock_client.evaluate_logrequest.return_value = {
            "log_request_evaluation": {"possible": True}
        }
        mock_client.create_logrequest.return_value = {
            "log_request": {"request_id": 999}
        }
        mock_client.get_logrequest_info.return_value = {
            "log_request": {
                "status": "processed",
                "parts": [{"part_number": 0, "size": 100}],
            }
        }
        first_part = (
            "watchID\tclientID\tdateTime\tparams\n"
            "123456\t98765\t2026-06-01 12:00:00\t{}\n"
        )
        mock_client.download_part.return_value = first_part.encode("utf-8")
        mock_client.clean_logrequest.return_value = {}

        rows = []
        async for row in stream_metrika_lines(
            token="fake",
            counter_id=1,
            date1="2026-06-01",
            date2="2026-06-01",
            source="hits",
            fields="ym:pv:watchID,ym:pv:clientID,ym:pv:dateTime,ym:pv:params",
        ):
            rows.append(row)

        assert len(rows) == 1
        assert rows[0] == {
            "watchID": "123456",
            "clientID": "98765",
            "dateTime": "2026-06-01 12:00:00",
            "params": "{}",
        }
        mock_client.clean_logrequest.assert_called_once()


# ----------------------------------------------------------------------
# Тесты основного процесса (через process_universal_etl)
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_process_yandex_metrika_success(valid_yandex_metrika_config, mock_repo):
    """Успешная обработка двух дней с мок-генератором."""

    async def mock_stream(*args, **kwargs):
        yield make_valid_row("1", "100", "2026-06-01T12:00:00", '{"key":"val"}')
        yield make_valid_row("2", "200", "2026-06-01T12:01:00", "")

    with patch("app.etl.transformer.stream_metrika_lines", side_effect=mock_stream):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            stats = await process_universal_etl(
                etl_config=valid_yandex_metrika_config,
                token="fake_token",
            )

    assert stats["lines_processed"] == 4  # по 2 строки за два дня
    assert stats["files_processed"] == 2
    # Проверяем, что транзакции коммитились
    assert mock_repo.commit.call_count == 2
    mock_repo.rollback.assert_not_called()


@pytest.mark.asyncio
async def test_process_yandex_metrika_unknown_field_error(
    valid_yandex_metrika_config, mock_repo
):
    """Появление неизвестного поля вызывает ProcessingInterrupted."""

    async def mock_stream(*args, **kwargs):
        yield {
            "watchID": "1",
            "clientID": "100",
            "dateTime": "2026-06-01T12:00:00",
            "params": "",
            "extraField": "oops",
        }

    with patch("app.etl.transformer.stream_metrika_lines", side_effect=mock_stream):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            with pytest.raises(ProcessingInterrupted) as exc:
                await process_universal_etl(
                    etl_config=valid_yandex_metrika_config,
                    token="fake_token",
                )
    assert "Unexpected fields" in str(exc.value)
    assert exc.value.failed_date == "2026-06-01"
    mock_repo.rollback.assert_called()
    mock_repo.commit.assert_not_called()


@pytest.mark.asyncio
async def test_process_yandex_metrika_validation_error(
    valid_yandex_metrika_config, mock_repo
):
    """Некорректные данные, которые не проходят валидацию MetrikaHitRow."""

    async def mock_stream(*args, **kwargs):
        # dateTime в неверном формате
        yield {
            "watchID": "abc",
            "clientID": "100",
            "dateTime": "bad_date",
            "params": "",
        }

    with patch("app.etl.transformer.stream_metrika_lines", side_effect=mock_stream):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            with pytest.raises(ProcessingInterrupted) as exc:
                await process_universal_etl(
                    etl_config=valid_yandex_metrika_config,
                    token="fake_token",
                )
    assert "Validation error" in str(exc.value)
    mock_repo.rollback.assert_called()


@pytest.mark.asyncio
async def test_resume_from_start_date(valid_yandex_metrika_config, mock_repo):
    """Проверка, что start_date пропускает дни до указанной даты."""

    async def mock_stream(*args, **kwargs):
        yield make_valid_row("1", "100", "2026-06-02T12:00:00")

    with patch("app.etl.transformer.stream_metrika_lines", side_effect=mock_stream):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            stats = await process_universal_etl(
                etl_config=valid_yandex_metrika_config,
                token="fake_token",
                source_params={"start_date": "2026-06-02"},
            )
    assert stats["files_processed"] == 1
    assert stats["lines_processed"] == 1


@pytest.mark.asyncio
async def test_transaction_rollback_on_db_error(valid_yandex_metrika_config, mock_repo):
    """Ошибка вставки в БД приводит к откату транзакции."""

    async def mock_stream(*args, **kwargs):
        yield make_valid_row("1", "100", "2026-06-01T12:00:00")

    mock_repo.insert_batch.side_effect = Exception("DB error")

    with patch("app.etl.transformer.stream_metrika_lines", side_effect=mock_stream):
        with patch("app.etl.transformer.get_repository", return_value=mock_repo):
            with pytest.raises(ProcessingInterrupted) as exc:
                await process_universal_etl(
                    etl_config=valid_yandex_metrika_config,
                    token="fake_token",
                )
    assert "DB error" in str(exc.value)
    mock_repo.rollback.assert_called()
    mock_repo.commit.assert_not_called()

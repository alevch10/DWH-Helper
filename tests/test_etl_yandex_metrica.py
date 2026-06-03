import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date
from app.etl.config_models import (
    ETLConfig,
    SourceYandexMetrikaConfig,
    TableConfig,
    FieldMapping,
)
from app.etl.transformer import process_universal_etl, ProcessingInterrupted
from app.yandex_metrika.schemas import MetrikaHitRow

# Пример конфига
CONFIG_YAML = """
source:
  type: yandex_metrika
  counter_id: 123
  date_from: "2026-06-01"
  date_to: "2026-06-01"
  source: hits
  fields:
    - ym:pv:watchID
    - ym:pv:clientID
tables:
  - name: test_table
    batch_size: 10
    fields:
      - target: watch_id
        sources: ["watchID"]
        type: integer
      - target: client_id
        sources: ["clientID"]
        type: integer
"""


@pytest.fixture
def etl_config():
    import yaml

    data = yaml.safe_load(CONFIG_YAML)
    return ETLConfig.model_validate(data)


@pytest.mark.asyncio
async def test_yandex_metrika_source_success(etl_config):
    # Мокаем stream_metrika_lines, чтобы возвращать заданные строки
    async def mock_stream(*args, **kwargs):
        yield {"watchID": "1", "clientID": "100"}
        yield {"watchID": "2", "clientID": "200"}

    with patch("app.etl.transformer.stream_metrika_lines", side_effect=mock_stream):
        with patch("app.etl.transformer.get_repository") as mock_repo:
            mock_repo.return_value.get_raw_connection.return_value = MagicMock()
            mock_repo.return_value.insert_batch.return_value = ([], 1)
            mock_repo.return_value.upsert_batch.return_value = ([], 1)
            stats = process_universal_etl(etl_config, token="fake_token")
            assert stats["lines_processed"] == 2
            assert stats["batches"]["test_table"] == 2  # две строки вставились


def test_config_parsing():
    import yaml

    data = yaml.safe_load(CONFIG_YAML)
    config = ETLConfig.model_validate(data)
    assert config.source.type == "yandex_metrika"
    assert config.source.counter_id == 123
    assert config.source.date_from == date(2026, 6, 1)
    assert len(config.tables[0].fields) == 2

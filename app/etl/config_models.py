from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any, Literal
from uuid import UUID


class SourceS3Sort(BaseModel):
    by: Literal["key", "last_modified"] = "key"
    natural: bool = True
    order: Literal["asc", "desc"] = "asc"


class SourceS3Config(BaseModel):
    type: Literal["s3"]
    bucket: str
    prefix: str
    file_pattern: str = "*.zip"
    sort: SourceS3Sort = Field(default_factory=SourceS3Sort)


# Для будущего: tmp_table
class SourceTmpTableConfig(BaseModel):
    type: Literal["tmp_table"]
    connection: str = "default"  # зарезервировано
    table_name: str
    batch_size: int = 1000


SourceConfig = SourceS3Config  # пока только S3


class FieldMapping(BaseModel):
    target: str
    sources: List[str] = Field(..., min_length=1)
    type: Literal[
        "string",
        "integer",
        "float",
        "boolean",
        "datetime",
        "json",
        "array",
        "inet",
        "uuid",
    ]
    required: bool = False
    default: Optional[Any] = None
    format: Optional[str] = None  # для datetime: "iso", "unix", "custom"
    value_map: Optional[Dict[str, Any]] = None
    null_values: List[str] = Field(default_factory=lambda: ["", "N/A", "null", "None"])
    max_length: Optional[int] = None  # для string
    min_value: Optional[int] = None  # для integer
    max_value: Optional[int] = None
    ignore: bool = False


from typing import List, Optional, Literal
from pydantic import BaseModel, Field
from app.etl.config_models import (
    FieldMapping,
)  # предполагается, что FieldMapping определён в этом же файле


class TableConfig(BaseModel):
    name: str = Field(
        ...,
        description="Имя таблицы (может включать схему, например 'amplitude_web.events')",
    )
    primary_key: Optional[List[str]] = Field(
        None, description="Список колонок первичного ключа (опционально)"
    )
    on_conflict: Optional[Literal["DO NOTHING", "DO UPDATE"]] = Field(
        None, description="Действие при конфликте: DO NOTHING или DO UPDATE"
    )
    update_strategy: Literal["always", "on_change"] = Field(
        "always",
        description="always — всегда вставлять; on_change — только если изменились значения",
    )
    ignore_fields_for_diff: List[str] = Field(
        default_factory=list,
        description="Список полей, игнорируемых при сравнении для on_change",
    )
    batch_size: int = Field(
        5000,
        description="Размер батча для массовой вставки (количество строк на один INSERT)",
    )
    fields: List[FieldMapping] = Field(..., description="Список маппингов полей")


class ETLConfig(BaseModel):
    source: SourceConfig
    tables: List[TableConfig]

    @field_validator("tables")
    @classmethod
    def validate_primary_key_conflict(cls, tables):
        for table in tables:
            if table.update_strategy == "on_change" and not table.primary_key:
                raise ValueError(
                    f"Table {table.name}: update_strategy=on_change requires primary_key"
                )
            if table.on_conflict == "DO UPDATE" and not table.primary_key:
                raise ValueError(
                    f"Table {table.name}: on_conflict=DO UPDATE requires primary_key"
                )
        return tables

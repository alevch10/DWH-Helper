from pydantic import BaseModel, Field
from typing import Optional, Dict


class SourceParams(BaseModel):
    start_after_file: Optional[str] = Field(
        None, description="Возобновить с указанного файла (ключ S3)"
    )
    start_after_line: Optional[int] = Field(
        0, description="Номер строки внутри файла (0-based)"
    )


class TransformRequest(BaseModel):
    config_yaml: str = Field(..., description="YAML-конфигурация ETL")
    source_params: Optional[SourceParams] = None


class TransformResponse(BaseModel):
    status: str  # success, interrupted
    message: str
    statistics: Optional[Dict[str, int]] = None
    failed_file: Optional[str] = None
    failed_line: Optional[int] = None
    last_successful_file: Optional[str] = None
    last_successful_line: Optional[int] = None
    error_details: Optional[str] = None

from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Literal, Optional


class PermanentUserProperties(BaseModel):
    """Статические свойства пользователя (одна запись на ehr_id)."""

    ehr_id: int = Field(..., description="EHR ID — первичный ключ")
    first_login_at: Optional[datetime] = None
    gender: Optional[str] = None
    cohort_day: Optional[int] = None
    cohort_week: Optional[int] = None
    cohort_month: Optional[int] = None
    registered_via_app: Optional[bool] = None
    start_version: Optional[str] = None
    source: Optional[str] = None


class ChangeableUserProperties(BaseModel):
    """Изменяемые свойства (последняя версия для ehr_id)."""

    ehr_id: Optional[int] = None
    uuid: UUID
    event_time: datetime
    language: Optional[str] = None
    age: Optional[int] = None
    app_city: Optional[str] = None
    push_permission: Optional[bool] = None
    location_permission: Optional[bool] = None
    authorization_status: Optional[bool] = None
    telemed_files_sent: Optional[int] = None
    appointments_cancelled: Optional[int] = None
    telemed_files_received: Optional[int] = None
    telemed_messages_received: Optional[int] = None
    telemed_messages_sent: Optional[int] = None
    telemed_consultations_resumed: Optional[int] = None
    appointments_booked: Optional[int] = None
    session_id: Optional[int] = None
    start_version: Optional[str] = None
    ehr_count: Optional[int] = None
    google_pay_available: Optional[bool] = None


class S3TransformRequest(BaseModel):
    source: Literal["s3"]
    bucket: str
    prefix: str
    start_after: Optional[str] = None


class TmpTableTransformRequest(BaseModel):
    source: Literal["tmp_table"]
    start_date: str
    interval: Literal["day", "week", "month"]


class TransformResponse(BaseModel):
    status: str
    message: str
    processed: int
    errors: int
    last_processed: Optional[str] = None


class ExportRequest(BaseModel):
    date_from: str = Field(
        ..., description="Дата начала (YYYY-MM-DD)", example="2024-01-01"
    )
    date_to: str = Field(
        ..., description="Дата конца (YYYY-MM-DD)", example="2024-01-31"
    )
    s3_dir: str = Field(
        ..., description="Директория внутри бакета S3", example="amplitude_exports/"
    )
    source: Literal["web", "mobile"] = Field(
        "web", description="Источник Amplitude: web или mobile"
    )


class ExportResult(BaseModel):
    s3_files: List[str] = Field(
        ..., description="Список путей файлов, загруженных в S3"
    )

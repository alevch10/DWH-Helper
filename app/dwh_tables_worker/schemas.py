from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any, List
from uuid import UUID


# =======================
# Request Schemas (POST)
# =======================


class EventsPart(BaseModel):
    uuid: UUID
    event_type: Optional[str] = None
    event_time: Optional[datetime] = None
    user_id: Optional[int] = None
    platform: Optional[str] = None
    device_id: Optional[str] = None
    event_id: Optional[int] = None
    language: Optional[str] = None
    os_name: Optional[str] = None
    os_version: Optional[str] = None
    session_id: Optional[int] = None
    start_version: Optional[str] = None
    version_name: Optional[str] = None

    @field_validator("event_time", mode="before")
    @classmethod
    def parse_event_time(cls, v):
        """Строго преобразует строку в datetime. Если формат неверный — ValueError."""
        if v is None:
            return v
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid datetime format for event_time: '{v}'. Expected ISO 8601."
            ) from e


class MobileDevices(BaseModel):
    device_id: str
    device_brand: Optional[str] = None
    device_carrier: Optional[str] = None
    device_family: Optional[str] = None
    device_manufacturer: Optional[str] = None
    device_model: Optional[str] = None
    device_type: Optional[str] = None


class PermanentUserProperties(BaseModel):
    ehr_id: int
    first_login_at: datetime
    gender: Optional[str] = None
    cohort_day: Optional[int] = None
    cohort_week: Optional[int] = None
    cohort_month: Optional[int] = None
    registered_via_app: Optional[bool] = None
    source: Optional[str] = None


class ChangeableUserProperties(BaseModel):
    ehr_id: Optional[int] = None
    uuid: UUID
    event_time: datetime  # обязательное поле
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

    @field_validator("event_time", mode="before")
    @classmethod
    def parse_event_time(cls, v):
        """Обязательное поле: строго преобразует строку в datetime."""
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, TypeError, AttributeError) as e:
            raise ValueError(
                f"Invalid datetime format for event_time: '{v}'. Expected ISO 8601."
            ) from e


class TechnicalData(BaseModel):
    uuid: UUID
    insert_id: Optional[str] = None
    amplitude_attribution_ids: Optional[Dict[str, Any]] = None
    amplitude_id: Optional[int] = None
    is_attribution_event: Optional[str] = None
    library: Optional[str] = None
    group_properties_json: Optional[Dict[str, Any]] = None
    groups_json: Optional[Dict[str, Any]] = None
    plan_json: Optional[Dict[str, Any]] = None


class TmpEventProperties(BaseModel):
    uuid: Optional[UUID] = None
    event_properties_json: Optional[Dict[str, Any]] = None


class TmpUserProperties(BaseModel):
    uuid: Optional[UUID] = None
    user_properties_json: Optional[Dict[str, Any]] = None
    language: Optional[str] = None
    session_id: Optional[int] = None
    start_version: Optional[str] = None
    migrated: Optional[bool] = None
    event_time: Optional[datetime] = None

    @field_validator("event_time", mode="before")
    @classmethod
    def parse_event_time(cls, v):
        """Если поле присутствует, оно должно быть валидным datetime или None."""
        if v is None:
            return v
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid datetime format for event_time: '{v}'. Expected ISO 8601."
            ) from e


class UserLocations(BaseModel):
    uuid: UUID
    location_lat: Optional[float] = None
    location_lng: Optional[float] = None
    city: Optional[str] = None
    country: Optional[str] = None
    ip_address: Optional[str] = None
    region: Optional[str] = None


# =======================
# Response Schemas (GET)
# =======================


class GetEventsPartResponse(BaseModel):
    rows: List[EventsPart]
    count: int


class GetMobileDevicesResponse(BaseModel):
    rows: List[MobileDevices]
    count: int


class GetPermanentUserPropertiesResponse(BaseModel):
    rows: List[PermanentUserProperties]
    count: int


class GetTechnicalDataResponse(BaseModel):
    rows: List[TechnicalData]
    count: int


class GetEventPropertiesResponse(BaseModel):
    rows: List[TmpEventProperties]
    count: int


class GetUserPropertiesResponse(BaseModel):
    rows: List[TmpUserProperties]
    count: int


class GetUserLocationsResponse(BaseModel):
    rows: List[UserLocations]
    count: int


# =======================
# Insert Response
# =======================


class InsertResponse(BaseModel):
    inserted_id: int


class BatchInsertResponse(BaseModel):
    inserted_ids: List[str] = Field(
        ..., description="Primary keys of inserted rows (as strings)"
    )
    count: int = Field(..., description="Total number of rows inserted")
    batches: int = Field(..., description="Number of batches used")


# =======================
# Batch Request Wrappers
# =======================


class EventsPartBatch(BaseModel):
    data: List[EventsPart]


class MobileDevicesBatch(BaseModel):
    data: List[MobileDevices]


class PermanentUserPropertiesBatch(BaseModel):
    data: List[PermanentUserProperties]


class TechnicalDataBatch(BaseModel):
    data: List[TechnicalData]


class TmpEventPropertiesBatch(BaseModel):
    data: List[TmpEventProperties]


class TmpUserPropertiesBatch(BaseModel):
    data: List[TmpUserProperties]


class UserLocationsBatch(BaseModel):
    data: List[UserLocations]


class ChangeableUserPropertiesBatch(BaseModel):
    """Batch request for changeable_user_properties."""

    data: List[ChangeableUserProperties]


class GetChangeableUserPropertiesResponse(BaseModel):
    """Response schema for GET /dwh/changeable-user-properties."""

    rows: List[ChangeableUserProperties]
    count: int

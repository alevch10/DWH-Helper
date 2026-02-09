from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from uuid import UUID
from datetime import datetime


# =======================
# Request Schemas (POST)
# =======================


class EventsPart(BaseModel):
    """Schema for events_part table - user events with device and session info."""
    uuid: UUID
    event_type: Optional[str] = None
    event_time: Optional[str] = None  # ISO timestamp
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


class MobileDevices(BaseModel):
    """Schema for mobile_devices table - device metadata."""
    device_id: str
    device_brand: Optional[str] = None
    device_carrier: Optional[str] = None
    device_family: Optional[str] = None
    device_manufacturer: Optional[str] = None
    device_model: Optional[str] = None
    device_type: Optional[str] = None


class PermanentUserProperties(BaseModel):
    """Schema for permanent_user_properties table - static user properties."""
    ehr_id: int = Field(..., description="EHR ID - primary key")
    gender: Optional[str] = None
    cohort_day: Optional[int] = None
    cohort_week: Optional[int] = None
    cohort_month: Optional[int] = None
    registered_via_app: Optional[bool] = None
    source: Optional[str] = None


class TechnicalData(BaseModel):
    """Schema for technical_data table - Amplitude service technical info."""
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
    """Schema for tmp_event_properties table - event properties as JSON."""
    uuid: Optional[UUID] = None
    event_properties_json: Optional[Dict[str, Any]] = None


class TmpUserProperties(BaseModel):
    """Schema for tmp_user_properties table - user properties (permanent and temporary)."""
    uuid: Optional[UUID] = None
    user_properties_json: Optional[Dict[str, Any]] = None
    language: Optional[str] = None
    session_id: Optional[int] = None
    start_version: Optional[str] = None
    migrated: Optional[bool] = None
    event_time: Optional[str] = None  # ISO timestamp


class UserLocations(BaseModel):
    """Schema for user_locations table - user geolocation data."""
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
    """Response schema for GET /dwh/events-part."""
    rows: List[EventsPart]
    count: int


class GetMobileDevicesResponse(BaseModel):
    """Response schema for GET /dwh/mobile-devices."""
    rows: List[MobileDevices]
    count: int


class GetPermanentUserPropertiesResponse(BaseModel):
    """Response schema for GET /dwh/permanent-user-properties."""
    rows: List[PermanentUserProperties]
    count: int


class GetTechnicalDataResponse(BaseModel):
    """Response schema for GET /dwh/technical-data."""
    rows: List[TechnicalData]
    count: int


class GetEventPropertiesResponse(BaseModel):
    """Response schema for GET /dwh/event-properties."""
    rows: List[TmpEventProperties]
    count: int


class GetUserPropertiesResponse(BaseModel):
    """Response schema for GET /dwh/user-properties."""
    rows: List[TmpUserProperties]
    count: int


class GetUserLocationsResponse(BaseModel):
    """Response schema for GET /dwh/user-locations."""
    rows: List[UserLocations]
    count: int


# =======================
# Insert Response
# =======================


class InsertResponse(BaseModel):
    """Response schema for single POST endpoints."""
    inserted_id: int = Field(..., description="ID of inserted row")


class BatchInsertResponse(BaseModel):
    """Response schema for batch POST endpoints."""
    inserted_ids: List[int] = Field(..., description="List of inserted row IDs")
    count: int = Field(..., description="Total number of rows inserted")
    batches: int = Field(..., description="Number of batches used (if data exceeded size limit)")


# =======================
# Batch Request Wrappers
# =======================


class EventsPartBatch(BaseModel):
    """Batch request for events_part."""
    data: List[EventsPart]


class MobileDevicesBatch(BaseModel):
    """Batch request for mobile_devices."""
    data: List[MobileDevices]


class PermanentUserPropertiesBatch(BaseModel):
    """Batch request for permanent_user_properties."""
    data: List[PermanentUserProperties]


class TechnicalDataBatch(BaseModel):
    """Batch request for technical_data."""
    data: List[TechnicalData]


class TmpEventPropertiesBatch(BaseModel):
    """Batch request for tmp_event_properties."""
    data: List[TmpEventProperties]


class TmpUserPropertiesBatch(BaseModel):
    """Batch request for tmp_user_properties."""
    data: List[TmpUserProperties]


class UserLocationsBatch(BaseModel):
    """Batch request for user_locations."""
    data: List[UserLocations]

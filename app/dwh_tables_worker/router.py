from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends

from app.auth.deps import require_read, require_write
from app.config.logger import get_logger

from .repository import DWHRepository
from .schemas import (
    EventsPart,
    MobileDevices,
    PermanentUserProperties,
    TechnicalData,
    TmpEventProperties,
    TmpUserProperties,
    UserLocations,
    EventsPartBatch,
    MobileDevicesBatch,
    PermanentUserPropertiesBatch,
    TechnicalDataBatch,
    TmpEventPropertiesBatch,
    TmpUserPropertiesBatch,
    UserLocationsBatch,
    GetEventsPartResponse,
    GetMobileDevicesResponse,
    GetPermanentUserPropertiesResponse,
    GetTechnicalDataResponse,
    GetEventPropertiesResponse,
    GetUserPropertiesResponse,
    GetUserLocationsResponse,
    BatchInsertResponse,
    ChangeableUserProperties,
    ChangeableUserPropertiesBatch,
    GetChangeableUserPropertiesResponse,
)

logger = get_logger(__name__)
router = APIRouter(tags=["DWH"])

# --- Dependency: DWH Repository (singleton) ---
_repo: Optional[DWHRepository] = None


def get_repo() -> DWHRepository:
    global _repo
    if _repo is None:
        _repo = DWHRepository()
        logger.info("DWHRepository initialized")
    return _repo


# --- Helper: convert Pydantic model list to dict list ---
def _models_to_dicts(models: List) -> List[dict]:
    return [item.model_dump(exclude_none=True) for item in models]


# =======================
# events_part endpoints
# =======================


@router.post("/events-part", response_model=BatchInsertResponse)
async def insert_events(
    request: EventsPartBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """Insert batch of events into events_part table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)
    inserted_ids, batches = repo.insert_batch(
        table="events_part", rows=rows, returning_column="uuid"
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get("/events-part", response_model=GetEventsPartResponse)
async def get_events(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get events from events_part table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {"uuid": pk} if pk else None
    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select("events_part", where=where, order_by=order_by, limit=limit)
    rows = [EventsPart(**row) for row in rows_data]

    return GetEventsPartResponse(rows=rows, count=len(rows))


# =======================
# mobile_devices endpoints
# =======================


@router.post("/mobile-devices", response_model=BatchInsertResponse)
async def insert_devices(
    request: MobileDevicesBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """Insert batch of devices into mobile_devices table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)
    inserted_ids, batches = repo.insert_batch(
        table="mobile_devices", rows=rows, returning_column="device_id"
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get("/mobile-devices", response_model=GetMobileDevicesResponse)
async def get_devices(
    pk: Optional[str] = Query(None, description="Filter by device_id"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get devices from mobile_devices table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {"device_id": pk} if pk else None
    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select(
        "mobile_devices", where=where, order_by=order_by, limit=limit
    )
    rows = [MobileDevices(**row) for row in rows_data]

    return GetMobileDevicesResponse(rows=rows, count=len(rows))


# =======================
# permanent_user_properties endpoints
# =======================


@router.post("/permanent-user-properties", response_model=BatchInsertResponse)
async def insert_user_properties(
    request: PermanentUserPropertiesBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """Insert batch of records into permanent_user_properties table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)
    inserted_ids, batches = repo.insert_batch(
        table="permanent_user_properties",
        rows=rows,
        on_conflict="DO NOTHING",
        conflict_target="(ehr_id)",
        returning_column="ehr_id",
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get(
    "/permanent-user-properties", response_model=GetPermanentUserPropertiesResponse
)
async def get_user_properties(
    pk: Optional[str] = Query(None, description="Filter by ehr_id"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get records from permanent_user_properties table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {"ehr_id": int(pk)} if pk else None
    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select(
        "permanent_user_properties", where=where, order_by=order_by, limit=limit
    )
    rows = [PermanentUserProperties(**row) for row in rows_data]

    return GetPermanentUserPropertiesResponse(rows=rows, count=len(rows))


# =======================
# changeable_data endpoints
# =======================


@router.post("/changeable-user-properties", response_model=BatchInsertResponse)
async def insert_changeable_user_properties(
    request: ChangeableUserPropertiesBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """
    Insert batch of records into changeable_user_properties table.
    UPSERT: if ehr_id exists (non-null) and conflicts, all columns except ehr_id are updated.
    Rows with ehr_id = NULL are always inserted.
    Returns list of inserted/updated UUIDs.
    """
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)

    # ON CONFLICT по ehr_id (только для не-NULL)
    set_clause = ", ".join(
        [
            f"{col} = EXCLUDED.{col}"
            for col in ChangeableUserProperties.model_fields.keys()
            if col != "ehr_id"
        ]
    )

    inserted_ids, batches = repo.insert_batch(
        table="changeable_user_properties",
        rows=rows,
        on_conflict=f"DO UPDATE SET {set_clause}",
        conflict_target="(ehr_id)",
        returning_column="uuid",  # ✅ возвращаем UUID, а не id
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get(
    "/changeable-user-properties", response_model=GetChangeableUserPropertiesResponse
)
async def get_changeable_user_properties(
    uuid: Optional[str] = Query(None, description="Filter by UUID (exact match)"),
    ehr_id: Optional[int] = Query(None, description="Filter by ehr_id"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(
        "event_time", description="Sort field (default: event_time)"
    ),
    sort_dir: str = Query(
        "desc", description="Sort direction (default: desc for latest first)"
    ),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get records from changeable_user_properties table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {}
    if uuid:
        where["uuid"] = uuid
    if ehr_id is not None:
        where["ehr_id"] = ehr_id

    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select(
        "changeable_user_properties",
        where=where or None,
        order_by=order_by,
        limit=limit,
    )
    rows = [ChangeableUserProperties(**row) for row in rows_data]

    return GetChangeableUserPropertiesResponse(rows=rows, count=len(rows))


# =======================
# technical_data endpoints
# =======================


@router.post("/technical-data", response_model=BatchInsertResponse)
async def insert_technical_data(
    request: TechnicalDataBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """Insert batch of records into technical_data table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)
    inserted_ids, batches = repo.insert_batch(
        table="technical_data", rows=rows, returning_column="uuid"
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get("/technical-data", response_model=GetTechnicalDataResponse)
async def get_technical_data(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get records from technical_data table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {"uuid": pk} if pk else None
    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select(
        "technical_data", where=where, order_by=order_by, limit=limit
    )
    rows = [TechnicalData(**row) for row in rows_data]

    return GetTechnicalDataResponse(rows=rows, count=len(rows))


# =======================
# event_properties endpoints (tmp_event_properties)
# =======================


@router.post("/event-properties", response_model=BatchInsertResponse)
async def insert_event_properties(
    request: TmpEventPropertiesBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """Insert batch of records into tmp_event_properties table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)
    inserted_ids, batches = repo.insert_batch(
        table="tmp_event_properties", rows=rows, returning_column="uuid"
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get("/event-properties", response_model=GetEventPropertiesResponse)
async def get_event_properties(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get records from tmp_event_properties table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {"uuid": pk} if pk else None
    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select(
        "tmp_event_properties", where=where, order_by=order_by, limit=limit
    )
    rows = [TmpEventProperties(**row) for row in rows_data]

    return GetEventPropertiesResponse(rows=rows, count=len(rows))


# =======================
# user_properties endpoints (tmp_user_properties)
# =======================


@router.post("/user-properties", response_model=BatchInsertResponse)
async def insert_user_properties_batch(
    request: TmpUserPropertiesBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """Insert batch of records into tmp_user_properties table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)
    inserted_ids, batches = repo.insert_batch(
        table="tmp_user_properties", rows=rows, returning_column="uuid"
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get("/user-properties", response_model=GetUserPropertiesResponse)
async def get_user_properties_tmp(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    migrated: Optional[bool] = Query(None, description="Filter by migrated flag"),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get records from tmp_user_properties table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {}
    if pk:
        where["uuid"] = pk
    if migrated is not None:
        where["migrated"] = migrated

    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select(
        "tmp_user_properties", where=where, order_by=order_by, limit=limit
    )
    rows = [TmpUserProperties(**row) for row in rows_data]

    return GetUserPropertiesResponse(rows=rows, count=len(rows))


# =======================
# user_locations endpoints
# =======================


@router.post("/user-locations", response_model=BatchInsertResponse)
async def insert_user_locations(
    request: UserLocationsBatch,
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_write),
):
    """Insert batch of records into user_locations table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")

    rows = _models_to_dicts(request.data)
    inserted_ids, batches = repo.insert_batch(
        table="user_locations", rows=rows, returning_column="uuid"
    )

    return BatchInsertResponse(
        inserted_ids=inserted_ids,
        count=len(inserted_ids),
        batches=batches,
    )


@router.get("/user-locations", response_model=GetUserLocationsResponse)
async def get_user_locations(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    repo: DWHRepository = Depends(get_repo),
    user=Depends(require_read),
):
    """Get records from user_locations table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")

    where = {"uuid": pk} if pk else None
    order_by = []
    if sort_by:
        prefix = "-" if sort_dir.lower() == "desc" else ""
        order_by.append(f"{prefix}{sort_by}")

    rows_data = repo.select(
        "user_locations", where=where, order_by=order_by, limit=limit
    )
    rows = [UserLocations(**row) for row in rows_data]

    return GetUserLocationsResponse(rows=rows, count=len(rows))

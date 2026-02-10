from fastapi import APIRouter, HTTPException, Query, Depends
from app.auth.deps import require_read, require_write
from typing import Optional, List
import json

router = APIRouter(tags=["DWH"])

from app.config.settings import settings
from .storage import storage
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
    InsertResponse,
    BatchInsertResponse,
)


def _chunk_by_size(data: List, schema_class, max_batch_bytes: int) -> List[List]:
    """Chunk data list by approximate byte size to respect DB limits.
    
    Args:
        data: List of dictionaries or Pydantic models
        schema_class: The schema class for serialization
        max_batch_bytes: Maximum bytes per chunk
        
    Returns:
        List of chunks (each chunk is a list of items)
    """
    chunks = []
    current_chunk = []
    current_size = 0
    
    for item in data:
        # Convert item to dict if it's a Pydantic model
        if hasattr(item, 'model_dump'):
            item_dict = item.model_dump(exclude_none=True)
        else:
            item_dict = item
            
        # Estimate size as JSON bytes
        item_json = json.dumps(item_dict, default=str)
        item_size = len(item_json.encode('utf-8'))
        
        # If single item exceeds limit, still add it (but warn)
        if item_size > max_batch_bytes and not current_chunk:
            current_chunk = [item]
            current_size = item_size
        elif current_size + item_size > max_batch_bytes and current_chunk:
            # Start new chunk
            chunks.append(current_chunk)
            current_chunk = [item]
            current_size = item_size
        else:
            # Add to current chunk
            current_chunk.append(item)
            current_size += item_size
    
    # Add remaining chunk
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks


def _insert_batch_data(table_name: str, data: List, schema_class, pk_field: str = None) -> tuple[List[int], int]:
    """Insert batch data with automatic chunking.
    
    Args:
        table_name: Name of the table
        data: List of items to insert
        schema_class: Pydantic schema class for items
        pk_field: Field name to extract PK value
        
    Returns:
        Tuple of (list of inserted IDs, total count)
    """
    max_bytes = settings.dwh.max_write_batch_bytes
    max_rows = settings.dwh.max_rows_per_insert
    
    # First, chunk by row count limit
    chunks_by_count = [data[i:i + max_rows] for i in range(0, len(data), max_rows)]
    
    all_inserted_ids = []
    
    for chunk in chunks_by_count:
        # Then chunk by size
        size_chunks = _chunk_by_size(chunk, schema_class, max_bytes)
        
        for size_chunk in size_chunks:
            for item in size_chunk:
                if hasattr(item, 'model_dump'):
                    item_dict = item.model_dump(exclude_none=True)
                else:
                    item_dict = item
                
                # Extract PK value if specified
                pk_value = None
                if pk_field and pk_field in item_dict:
                    pk_value = str(item_dict[pk_field])
                elif pk_field and hasattr(item, pk_field):
                    pk_value = str(getattr(item, pk_field))
                
                # Insert row
                row_id = storage.insert_row(table_name, item_dict, pk_value=pk_value)
                all_inserted_ids.append(row_id)
    
    return all_inserted_ids, len(all_inserted_ids)


# =======================
# events_part endpoints
# =======================


@router.post("/events-part", response_model=BatchInsertResponse)
async def insert_events(request: EventsPartBatch, user=Depends(require_write)):
    """Insert batch of events into events_part table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")
    
    inserted_ids, count = _insert_batch_data(
        "events_part",
        request.data,
        EventsPart,
        pk_field="uuid"
    )
    
    num_batches = max(1, len(request.data) // settings.dwh.max_rows_per_insert)
    return {
        "inserted_ids": inserted_ids,
        "count": count,
        "batches": num_batches
    }


@router.get("/events-part", response_model=GetEventsPartResponse)
async def get_events(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    user=Depends(require_read)
):
    """Get events from events_part table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")
    rows_data = storage.query_rows("events_part", pk=pk, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
    rows = [EventsPart(**row['row']) for row in rows_data]
    return {"rows": rows, "count": len(rows)}


# =======================
# mobile_devices endpoints
# =======================


@router.post("/mobile-devices", response_model=BatchInsertResponse)
async def insert_devices(request: MobileDevicesBatch, user=Depends(require_write)):
    """Insert batch of devices into mobile_devices table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")
    
    inserted_ids, count = _insert_batch_data(
        "mobile_devices",
        request.data,
        MobileDevices,
        pk_field="device_id"
    )
    
    num_batches = max(1, len(request.data) // settings.dwh.max_rows_per_insert)
    return {
        "inserted_ids": inserted_ids,
        "count": count,
        "batches": num_batches
    }


@router.get("/mobile-devices", response_model=GetMobileDevicesResponse)
async def get_devices(
    pk: Optional[str] = Query(None, description="Filter by device_id"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    user=Depends(require_read)
):
    """Get devices from mobile_devices table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")
    rows_data = storage.query_rows("mobile_devices", pk=pk, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
    rows = [MobileDevices(**row['row']) for row in rows_data]
    return {"rows": rows, "count": len(rows)}


# =======================
# permanent_user_properties endpoints
# =======================


@router.post("/permanent-user-properties", response_model=BatchInsertResponse)
async def insert_user_properties(request: PermanentUserPropertiesBatch, user=Depends(require_write)):
    """Insert batch of records into permanent_user_properties table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")
    
    inserted_ids, count = _insert_batch_data(
        "permanent_user_properties",
        request.data,
        PermanentUserProperties,
        pk_field="ehr_id"
    )
    
    num_batches = max(1, len(request.data) // settings.dwh.max_rows_per_insert)
    return {
        "inserted_ids": inserted_ids,
        "count": count,
        "batches": num_batches
    }


@router.get("/permanent-user-properties", response_model=GetPermanentUserPropertiesResponse)
async def get_user_properties(
    pk: Optional[str] = Query(None, description="Filter by ehr_id"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    user=Depends(require_read)
):
    """Get records from permanent_user_properties table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")
    rows_data = storage.query_rows("permanent_user_properties", pk=pk, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
    rows = [PermanentUserProperties(**row['row']) for row in rows_data]
    return {"rows": rows, "count": len(rows)}


# =======================
# technical_data endpoints
# =======================


@router.post("/technical-data", response_model=BatchInsertResponse)
async def insert_technical_data(request: TechnicalDataBatch, user=Depends(require_write)):
    """Insert batch of records into technical_data table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")
    
    inserted_ids, count = _insert_batch_data(
        "technical_data",
        request.data,
        TechnicalData,
        pk_field="uuid"
    )
    
    num_batches = max(1, len(request.data) // settings.dwh.max_rows_per_insert)
    return {
        "inserted_ids": inserted_ids,
        "count": count,
        "batches": num_batches
    }


@router.get("/technical-data", response_model=GetTechnicalDataResponse)
async def get_technical_data(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    user=Depends(require_read)
):
    """Get records from technical_data table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")
    rows_data = storage.query_rows("technical_data", pk=pk, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
    rows = [TechnicalData(**row['row']) for row in rows_data]
    return {"rows": rows, "count": len(rows)}


# =======================
# event_properties endpoints
# =======================


@router.post("/event-properties", response_model=BatchInsertResponse)
async def insert_event_properties(request: TmpEventPropertiesBatch, user=Depends(require_write)):
    """Insert batch of records into tmp_event_properties table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")
    
    inserted_ids, count = _insert_batch_data(
        "tmp_event_properties",
        request.data,
        TmpEventProperties,
        pk_field="uuid"
    )
    
    num_batches = max(1, len(request.data) // settings.dwh.max_rows_per_insert)
    return {
        "inserted_ids": inserted_ids,
        "count": count,
        "batches": num_batches
    }


@router.get("/event-properties", response_model=GetEventPropertiesResponse)
async def get_event_properties(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    user=Depends(require_read)
):
    """Get records from tmp_event_properties table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")
    rows_data = storage.query_rows("tmp_event_properties", pk=pk, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
    rows = [TmpEventProperties(**row['row']) for row in rows_data]
    return {"rows": rows, "count": len(rows)}


# =======================
# user_properties endpoints
# =======================


@router.post("/user-properties", response_model=BatchInsertResponse)
async def insert_user_properties_batch(request: TmpUserPropertiesBatch, user=Depends(require_write)):
    """Insert batch of records into tmp_user_properties table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")
    
    inserted_ids, count = _insert_batch_data(
        "tmp_user_properties",
        request.data,
        TmpUserProperties,
        pk_field="uuid"
    )
    
    num_batches = max(1, len(request.data) // settings.dwh.max_rows_per_insert)
    return {
        "inserted_ids": inserted_ids,
        "count": count,
        "batches": num_batches
    }


@router.get("/user-properties", response_model=GetUserPropertiesResponse)
async def get_user_properties_tmp(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    user=Depends(require_read)
):
    """Get records from tmp_user_properties table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")
    rows_data = storage.query_rows("tmp_user_properties", pk=pk, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
    rows = [TmpUserProperties(**row['row']) for row in rows_data]
    return {"rows": rows, "count": len(rows)}


# =======================
# user_locations endpoints
# =======================


@router.post("/user-locations", response_model=BatchInsertResponse)
async def insert_user_locations(request: UserLocationsBatch, user=Depends(require_write)):
    """Insert batch of records into user_locations table."""
    if not request.data:
        raise HTTPException(status_code=400, detail="data array cannot be empty")
    
    inserted_ids, count = _insert_batch_data(
        "user_locations",
        request.data,
        UserLocations,
        pk_field="uuid"
    )
    
    num_batches = max(1, len(request.data) // settings.dwh.max_rows_per_insert)
    return {
        "inserted_ids": inserted_ids,
        "count": count,
        "batches": num_batches
    }


@router.get("/user-locations", response_model=GetUserLocationsResponse)
async def get_user_locations(
    pk: Optional[str] = Query(None, description="Filter by UUID"),
    limit: Optional[int] = Query(None, description="Max rows to return"),
    sort_by: Optional[str] = Query(None, description="Sort field"),
    sort_dir: str = Query("asc", description="Sort direction"),
    user=Depends(require_read)
):
    """Get records from user_locations table."""
    if sort_dir.lower() not in ("asc", "desc"):
        raise HTTPException(status_code=400, detail="sort_dir must be 'asc' or 'desc'")
    rows_data = storage.query_rows("user_locations", pk=pk, limit=limit, sort_by=sort_by, sort_dir=sort_dir)
    rows = [UserLocations(**row['row']) for row in rows_data]
    return {"rows": rows, "count": len(rows)}

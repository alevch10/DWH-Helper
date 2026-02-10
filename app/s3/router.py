"""FastAPI router for S3 storage operations."""

import logging
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Depends
from app.auth.deps import require_read, require_write
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

from app.s3.client import S3Client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/s3", tags=["S3"])

# Initialize S3 client (singleton)
s3_client = S3Client()


# =======================
# Request/Response Models
# =======================
class S3ObjectInfo(BaseModel):
    """Information about an S3 object."""
    key: str
    size: Optional[int] = None
    exists: bool = False


class S3ObjectList(BaseModel):
    """List of S3 objects."""
    prefix: str
    objects: List[Dict[str, Any]]
    count: int


class S3UploadResponse(BaseModel):
    """Response after uploading object to S3."""
    key: str
    etag: Optional[str] = None
    version_id: Optional[str] = None
    size: int


class S3DeleteResponse(BaseModel):
    """Response after deleting object from S3."""
    key: str
    deleted: bool


# =======================
# GET Endpoints
# =======================
@router.get("/objects", response_model=S3ObjectList)
async def list_objects(prefix: str = Query("", description="S3 prefix/folder path"), user=Depends(require_read)):
    """
    List objects in S3 with optional prefix.
    
    Returns direct files only (no nested subdirectories).
    """
    try:
        objects = s3_client.list_objects(prefix=prefix)
        return {
            "prefix": prefix,
            "objects": objects,
            "count": len(objects),
        }
    except Exception as e:
        logger.error(f"Error listing S3 objects: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listing objects: {str(e)}")


@router.get("/object-info", response_model=S3ObjectInfo)
async def get_object_info(key: str = Query(..., description="S3 object key"), user=Depends(require_read)):
    """Get information about an S3 object (size, existence)."""
    try:
        exists = s3_client.object_exists(key)
        size = s3_client.get_object_size(key) if exists else None
        return {
            "key": key,
            "size": size,
            "exists": exists,
        }
    except Exception as e:
        logger.error(f"Error getting object info: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting object info: {str(e)}")


@router.get("/download")
async def download_object(key: str = Query(..., description="S3 object key"), user=Depends(require_read)):
    """
    Download object from S3.
    
    Returns the file content with appropriate headers.
    """
    try:
        if not s3_client.object_exists(key):
            raise HTTPException(status_code=404, detail=f"Object not found: {key}")
        
        data = s3_client.get_object(key)
        return {
            "key": key,
            "size": len(data),
            "data": data.hex(),  # Return as hex for JSON serialization
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading object: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error downloading object: {str(e)}")


# =======================
# POST Endpoints
# =======================
@router.post("/upload", response_model=S3UploadResponse)
async def upload_object(
    key: str = Query(..., description="S3 object key"),
    file: UploadFile = File(..., description="File to upload"),
    user=Depends(require_write),
):
    """
    Upload file to S3.
    
    Args:
        key: S3 object key/path
        file: File content
    """
    try:
        content = await file.read()
        content_type = file.content_type or "application/octet-stream"
        
        result = s3_client.put_object(key, content, content_type=content_type)
        return {
            "key": result["Key"],
            "etag": result.get("ETag"),
            "version_id": result.get("VersionId"),
            "size": result["Size"],
        }
    except Exception as e:
        logger.error(f"Error uploading object: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading object: {str(e)}")


@router.post("/upload-binary")
async def upload_binary(key: str = Query(..., description="S3 object key"), user=Depends(require_write)):
    """
    Upload binary data to S3 from request body.
    
    Args:
        key: S3 object key/path
        
    Body: Raw binary data
    """
    try:
        # This endpoint receives raw binary data
        # In a real scenario, you'd read from request.body or similar
        raise HTTPException(status_code=501, detail="Use /upload endpoint with multipart form data")
    except Exception as e:
        logger.error(f"Error uploading binary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error uploading: {str(e)}")


# =======================
# PUT Endpoints
# =======================
@router.put("/update", response_model=S3UploadResponse)
async def update_object(
    key: str = Query(..., description="S3 object key"),
    file: UploadFile = File(..., description="File content"),
    user=Depends(require_write),
):
    """
    Update existing object in S3 (PUT).
    
    Args:
        key: S3 object key/path
        file: New file content
    """
    try:
        content = await file.read()
        content_type = file.content_type or "application/octet-stream"
        
        # Check if object exists
        if not s3_client.object_exists(key):
            logger.warning(f"Object does not exist: {key}. Creating new object.")
        
        result = s3_client.update_object(key, content, content_type=content_type)
        return {
            "key": result["Key"],
            "etag": result.get("ETag"),
            "version_id": result.get("VersionId"),
            "size": result["Size"],
        }
    except Exception as e:
        logger.error(f"Error updating object: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error updating object: {str(e)}")


# =======================
# PATCH Endpoints
# =======================
@router.patch("/patch", response_model=S3UploadResponse)
async def patch_object(
    key: str = Query(..., description="S3 object key"),
    file: UploadFile = File(..., description="Data to patch"),
    offset: int = Query(0, description="Offset position"),
    user=Depends(require_write),
):
    """
    Patch (partial update) object in S3.
    
    Note: S3 doesn't support true PATCH. This operation replaces/appends data.
    
    Args:
        key: S3 object key/path
        file: Data to append/write
        offset: Offset position (informational)
    """
    try:
        content = await file.read()
        content_type = file.content_type or "application/octet-stream"
        
        if not s3_client.object_exists(key):
            raise HTTPException(status_code=404, detail=f"Object not found: {key}")
        
        result = s3_client.patch_object(key, content, offset=offset)
        return {
            "key": result["Key"],
            "etag": result.get("ETag"),
            "version_id": result.get("VersionId"),
            "size": result["Size"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error patching object: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error patching object: {str(e)}")


# =======================
# DELETE Endpoints
# =======================
@router.delete("/delete", response_model=S3DeleteResponse)
async def delete_object(key: str = Query(..., description="S3 object key"), user=Depends(require_write)):
    """
    Delete object from S3.
    
    Args:
        key: S3 object key/path
    """
    try:
        if not s3_client.object_exists(key):
            raise HTTPException(status_code=404, detail=f"Object not found: {key}")
        
        s3_client.delete_object(key)
        return {
            "key": key,
            "deleted": True,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting object: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting object: {str(e)}")


__all__ = ["router", "s3_client"]

"""AWS S3 client for managing object storage operations."""

import boto3
import logging
from botocore.config import Config
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.config.settings import settings

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self):
        logger.info("Initializing S3 client")
        # Клиент для операций GET (требует s3v4)
        self.client_v4 = boto3.client(
            "s3",
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
            region_name=settings.s3.region,
            endpoint_url=settings.s3.endpoint_url,
            config=Config(
                signature_version="s3v4",  # Для GET операций
                s3={
                    "addressing_style": "path",
                },
            ),
        )
        
        # Клиент для остальных операций (POST, PUT, DELETE)
        self.client = boto3.client(
            "s3",
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
            region_name=settings.s3.region,
            endpoint_url=settings.s3.endpoint_url,
            config=Config(
                signature_version="s3",  # Для остальных операций
                s3={
                    "addressing_style": "path",
                    "payload_signing_enabled": False,
                },
            ),
        )
        
        self.bucket = settings.s3.bucket_name
        logger.info(f"S3 client initialized for bucket: {self.bucket}")

    def list_objects(self, prefix: str = "") -> List[Dict[str, Any]]:
        """
        List objects with prefix, return sorted list of objects (oldest first).
        
        Only returns direct files under prefix (no nested subdirectories).
        
        Args:
            prefix: S3 prefix/folder path
            
        Returns:
            List of dicts with 'Key' (str) and 'LastModified' (datetime)
        """
        logger.info(f"Listing S3 objects with prefix: {prefix}")
        objects = []
        paginator = self.client_v4.get_paginator("list_objects_v2")
        
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" in page:
                objects.extend(page["Contents"])
        
        # Filter to only direct files: no additional / after prefix
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        
        filtered_objects = []
        for obj in objects:
            # Skip folder placeholders (ending with /)
            if obj["Key"].endswith("/"):
                continue
            # Only include direct files (same depth as prefix)
            if prefix:
                if obj["Key"].startswith(prefix) and obj["Key"].count("/") == prefix.count("/"):
                    filtered_objects.append(obj)
            else:
                # No prefix: only files in root
                if "/" not in obj["Key"]:
                    filtered_objects.append(obj)
        
        # Sort by LastModified ascending (oldest first)
        filtered_objects.sort(key=lambda x: x["LastModified"], reverse=False)
        
        result = [
            {"Key": obj["Key"], "LastModified": obj["LastModified"]}
            for obj in filtered_objects
        ]
        logger.info(f"Found {len(result)} direct files in prefix '{prefix}'")
        return result

    def get_object(self, key: str) -> bytes:
        """
        Download object as bytes.
        
        Args:
            key: S3 object key/path
            
        Returns:
            Object data as bytes
        """
        logger.info(f"Downloading S3 object: {key}")
        response = self.client_v4.get_object(Bucket=self.bucket, Key=key)
        data = response["Body"].read()
        logger.info(f"Downloaded {key}, size: {len(data)} bytes")
        return data

    def put_object(self, key: str, data: bytes, content_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Upload object to S3.
        
        Args:
            key: S3 object key/path
            data: Object data as bytes
            content_type: MIME type (optional, defaults to application/octet-stream)
            
        Returns:
            Response metadata from S3
        """
        logger.info(f"Uploading S3 object: {key}, size: {len(data)} bytes")
        
        kwargs = {"Bucket": self.bucket, "Key": key, "Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        
        response = self.client.put_object(**kwargs)
        logger.info(f"Successfully uploaded {key}")
        return {
            "Key": key,
            "ETag": response.get("ETag"),
            "VersionId": response.get("VersionId"),
            "Size": len(data),
        }

    def post_object(self, key: str, data: bytes, content_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Alias for put_object - upload/create object to S3.
        
        Args:
            key: S3 object key/path
            data: Object data as bytes
            content_type: MIME type (optional)
            
        Returns:
            Response metadata from S3
        """
        return self.put_object(key, data, content_type)

    def update_object(self, key: str, data: bytes, content_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Update existing object in S3 (same as PUT).
        
        Args:
            key: S3 object key/path
            data: Object data as bytes
            content_type: MIME type (optional)
            
        Returns:
            Response metadata from S3
        """
        logger.info(f"Updating S3 object: {key}")
        return self.put_object(key, data, content_type)

    def patch_object(self, key: str, data: bytes, offset: int = 0) -> Dict[str, Any]:
        """
        Patch (partial update) of object in S3.
        
        Note: Standard S3 doesn't support true PATCH. This appends data or replaces.
        For S3-compatible storage, behavior may vary.
        
        Args:
            key: S3 object key/path
            data: Data to append/write
            offset: Offset position (informational, not used in standard S3)
            
        Returns:
            Response metadata from S3
        """
        logger.info(f"Patching S3 object: {key} at offset {offset}")
        return self.put_object(key, data)

    def delete_object(self, key: str) -> Dict[str, Any]:
        """
        Delete object from S3.
        
        Args:
            key: S3 object key/path
            
        Returns:
            Response metadata from S3
        """
        logger.info(f"Deleting S3 object: {key}")
        response = self.client.delete_object(Bucket=self.bucket, Key=key)
        logger.info(f"Successfully deleted {key}")
        return {
            "Key": key,
            "DeleteMarker": response.get("DeleteMarker"),
            "VersionId": response.get("VersionId"),
        }

    def object_exists(self, key: str) -> bool:
        """
        Check if object exists in S3.
        
        Args:
            key: S3 object key/path
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            self.client_v4.head_object(Bucket=self.bucket, Key=key)
            logger.info(f"Object exists: {key}")
            return True
        except Exception as e:
            logger.debug(f"Object not found: {key} - {str(e)}")
            return False

    def get_object_size(self, key: str) -> Optional[int]:
        """
        Get size of object in bytes.
        
        Args:
            key: S3 object key/path
            
        Returns:
            Size in bytes, or None if object doesn't exist
        """
        try:
            response = self.client_v4.head_object(Bucket=self.bucket, Key=key)
            size = response.get("ContentLength")
            logger.info(f"Object size: {key} = {size} bytes")
            return size
        except Exception as e:
            logger.debug(f"Could not get size for {key}: {str(e)}")
            return None

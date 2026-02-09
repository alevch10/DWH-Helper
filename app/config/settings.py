from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel
from typing import Optional


# =======================
# DWH (Database) Settings
# =======================
class DWHSettings(BaseModel):
    """Data Warehouse settings."""
    database_url: str = "sqlite:///./dwh.db"
    # PostgreSQL batch write limit in bytes (affects how many rows per INSERT)
    # Set to ~10MB to avoid exceeding typical PostgreSQL limits
    max_write_batch_bytes: int = 10_000_000  # ~10 MB
    # Max number of rows per single INSERT statement
    max_rows_per_insert: int = 1000


# =======================
# AppMetrica API Settings
# =======================
class AppMetricaSettings(BaseModel):
    """AppMetrica integration settings."""
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    application_id: Optional[int] = None
    poll_interval_seconds: int = 5
    poll_timeout_seconds: int = 300


# =======================
# S3 Storage Settings
# =======================
class S3Settings(BaseModel):
    """AWS S3 storage settings."""
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None
    bucket_name: str = "default-bucket"


# =======================
# Logging Settings
# =======================
class LoggingSettings(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# =======================
# Main Settings
# =======================
class Settings(BaseSettings):
    """Application settings."""
    # Application metadata
    title: str = "DWH Helper"
    version: str = "1.0.0"
    description: str = "Product analytics data ingestion and DWH integration"
    debug: bool = False

    # Settings blocks
    dwh: DWHSettings = DWHSettings()
    appmetrica: AppMetricaSettings = AppMetricaSettings()
    s3: S3Settings = S3Settings()
    logging: LoggingSettings = LoggingSettings()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="_",
        case_sensitive=False,
        extra="ignore",  # Ignore extra environment variables from old config
    )


settings = Settings()


__all__ = ["Settings", "settings", "DWHSettings", "AppMetricaSettings", "S3Settings", "LoggingSettings"]

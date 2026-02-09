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
    base_url: str
    api_key: str
    application_id: str
    poll_interval_seconds: int
    poll_timeout_seconds: int


# =======================
# S3 Storage Settings
# =======================
class S3Settings(BaseModel):
    """AWS S3 storage settings."""
    access_key_id: str
    secret_access_key: str
    region: str
    endpoint_url: str
    bucket_name: str


# =======================
# Logging Settings
# =======================
class LoggingSettings(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# =======================
# Amplitude API Settings
# =======================
class AmplitudeSettings(BaseModel):
    """Amplitude API credentials for web and mobile sources."""
    web_secret_key: str
    web_client_id: str
    mobile_secret_key: str
    mobile_client_id: str


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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        env_file_encoding="utf-8",
        env_nested_delimiter="_",
        env_nested_max_split=1,
        case_sensitive=False,
        extra="ignore",
    )

    dwh: DWHSettings
    appmetrica: AppMetricaSettings
    s3: S3Settings
    logging: LoggingSettings
    amplitude: AmplitudeSettings


settings = Settings()

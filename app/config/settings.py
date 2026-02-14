from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field, field_validator


class DBSettings(BaseModel):
    """Data Warehouse settings."""

    name: str
    user: str
    password: str
    host: str
    port: int
    max_params_per_query: int
    max_rows_per_insert: int
    safety_factor: float
    minconn: int = 1
    maxconn: int = 10


class AppMetricaSettings(BaseModel):
    """AppMetrica integration settings."""

    base_url: str
    application_id: str
    poll_interval_seconds: int
    poll_timeout_seconds: int


class YandexOAuthSettings(BaseModel):
    """Yandex OAuth settings for user authentication."""

    client_id: str
    client_secret: str


class S3Settings(BaseModel):
    """AWS S3 storage settings."""

    access_key_id: str
    secret_access_key: str
    region: str
    endpoint_url: str
    bucket_name: str


class LoggingSettings(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class AmplitudeSettings(BaseModel):
    """Amplitude API credentials for web and mobile sources."""

    web_secret_key: str
    web_client_id: str
    mobile_secret_key: str
    mobile_client_id: str


class ETLSettings(BaseModel):
    batch_size: int


class Settings(BaseSettings):
    """Application settings."""

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

    db: DBSettings
    appmetrica: AppMetricaSettings
    s3: S3Settings
    logging: LoggingSettings
    amplitude: AmplitudeSettings
    yandex: YandexOAuthSettings
    etl: ETLSettings
    read_access: str = Field(default="", validation_alias="AUTH_READ_ACCESS")
    write_access: str = Field(default="", validation_alias="AUTH_WRITE_ACCESS")

    @field_validator("read_access", "write_access", mode="after")
    @classmethod
    def parse_access_lists(cls, v):
        """Ensure read_access and write_access are strings (will parse on access)."""
        return v if isinstance(v, str) else ""

    def get_read_access_list(self) -> list[str]:
        """Get read_access as list of strings."""
        if not self.read_access:
            return []
        return [x.strip() for x in self.read_access.split(",") if x.strip()]

    def get_write_access_list(self) -> list[str]:
        """Get write_access as list of strings."""
        if not self.write_access:
            return []
        return [x.strip() for x in self.write_access.split(",") if x.strip()]


settings = Settings()

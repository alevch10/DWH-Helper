from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "DWH Helper"
    debug: bool = False
    log_level: str = "INFO"
    dwh_database_url: str = "sqlite:///./dwh.db"
    appmetrica_base_url: Optional[str] = None
    appmetrica_api_key: Optional[str] = None
    appmetrica_application_id: Optional[str] = None


settings = Settings()


__all__ = ["Settings", "settings"]

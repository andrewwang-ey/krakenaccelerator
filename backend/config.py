"""Centralised settings, loaded from .env locally and from App Settings in Azure."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    azure_sql_server: str
    azure_sql_database: str
    azure_storage_account: str
    port: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()

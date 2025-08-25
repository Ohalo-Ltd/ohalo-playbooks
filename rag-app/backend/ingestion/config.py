"""
Configuration for document ingestion operations (uses superuser privileges)
"""

from pydantic_settings import BaseSettings
from typing import List


class IngestionSettings(BaseSettings):
    """Configuration for document ingestion operations"""

    # Database - use postgres superuser for imports (bypasses RLS)
    database_url: str = "postgresql://postgres:password@localhost:5432/ragapp"

    # API Keys
    dxr_api_key: str = ""
    dxr_api_url: str = "https://dev.dataxray.io/api"
    dxr_datasource_id: str = ""
    openai_api_key: str = ""

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


# Ingestion settings instance
ingestion_settings = IngestionSettings()

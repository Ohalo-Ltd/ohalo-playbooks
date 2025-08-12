"""
Configuration management for the RAG application
"""

from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """Application configuration"""

    # Database - use non-privileged user for RAG operations
    database_url: str = "postgresql://ragapp_user:ragapp_password@localhost:5432/ragapp"

    # API Keys
    dxr_api_key: str = ""
    dxr_api_url: str = "https://dev.dataxray.io/api"
    dxr_datasource_id: str = ""
    openai_api_key: str = ""

    # Application
    backend_port: int = 8000
    environment: str = "development"

    # Security
    allowed_origins: str = "http://localhost:3000"

    # DXR Extractor
    dxr_extractor_id: str = "1"

    def get_allowed_origins(self) -> List[str]:
        """Get list of allowed CORS origins"""
        return [origin.strip() for origin in self.allowed_origins.split(",")]

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"  # This allows extra fields to be ignored


# Global settings instance
settings = Settings()

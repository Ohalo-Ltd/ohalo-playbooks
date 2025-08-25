"""
Configuration management for the RAG application
Handles both environment variables and YAML config loading
"""

import yaml
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass
from pydantic_settings import BaseSettings
from core.logging import get_logger

logger = get_logger(__name__)

@dataclass
class ExtractedMetadataConfig:
    column_name: str
    column_type: str

@dataclass
class UserConfig:
    email: str
    name: str
    role: str
    employee_id: str
    groups: List[str]

class Settings(BaseSettings):
    """Application configuration (env vars)"""
    database_url: str = "postgresql://ragapp_user:ragapp_password@localhost:5432/ragapp"
    dxr_api_key: str = ""
    dxr_api_url: str = "https://dev.dataxray.io/api"
    dxr_datasource_id: str = ""
    openai_api_key: str = ""
    backend_port: int = 8000
    environment: str = "development"
    allowed_origins: str = "http://localhost:3000"
    dxr_employee_id_extractor_id: str = "1"

    def get_allowed_origins(self) -> List[str]:
        return [origin.strip() for origin in self.allowed_origins.split(",")]

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"

class YAMLConfig:
    """YAML configuration loader for users and extracted metadata"""
    def __init__(self, config_path: str | None = None):
        if config_path is None:
            backend_dir = Path(__file__).parent.parent
            rag_app_root = backend_dir.parent
            config_path = rag_app_root / "config.yml"
        self.config_path = Path(config_path)
        self._config_data = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        try:
            if not self.config_path.exists():
                logger.warning(f"⚠️  Config file not found: {self.config_path}")
                return {}
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
            logger.info(f"✅ Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"❌ Failed to load configuration: {e}")
            return {}

    def get_users(self) -> Dict[str, UserConfig]:
        users_data = self._config_data.get("users", {})
        users = {}
        for email, user_data in users_data.items():
            try:
                users[email] = UserConfig(
                    email=user_data.get("email", email),
                    name=user_data.get("name", ""),
                    role=user_data.get("role", ""),
                    employee_id=user_data.get("employee_id", ""),
                    groups=user_data.get("groups", [])
                )
            except Exception as e:
                logger.warning(f"⚠️  Failed to parse user config for {email}: {e}")
        return users

    def get_extracted_metadata_config(self) -> Dict[str, ExtractedMetadataConfig]:
        metadata_list = self._config_data.get("extracted_metadata", [])
        metadata_configs = {}
        for item in metadata_list:
            try:
                extractor_id = str(item["id"])
                metadata_configs[extractor_id] = ExtractedMetadataConfig(
                    column_name=item["column_name"],
                    column_type=item["column_type"]
                )
            except Exception as e:
                logger.warning(f"⚠️  Failed to parse metadata config for extractor {item}: {e}")
        return metadata_configs

    def get_metadata_columns_for_database(self) -> List[tuple]:
        metadata_configs = self.get_extracted_metadata_config()
        columns = []
        for config in metadata_configs.values():
            pg_type = self._map_to_postgres_type(config.column_type)
            columns.append((config.column_name, pg_type))
        return columns

    def _map_to_postgres_type(self, yaml_type: str) -> str:
        type_mapping = {
            "TEXT": "TEXT",
            "NUMBER": "NUMERIC",
            "NUMERIC": "NUMERIC", 
            "INTEGER": "INTEGER",
            "JSON": "JSONB",
            "JSONB": "JSONB",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP"
        }
        return type_mapping.get(yaml_type.upper(), "TEXT")

# Global settings and YAML config instances
settings = Settings()
yaml_config = YAMLConfig()

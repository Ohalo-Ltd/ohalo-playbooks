import os
from typing import Optional, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration management for SMTP proxy."""
    
    def __init__(self):
        self.dataxray_base_url = os.getenv("DATAXRAY_BASE_URL", "")
        self.dataxray_api_key = os.getenv("DATAXRAY_API_KEY", "")
        self.datasource_ids = self._parse_datasource_ids(os.getenv("DATASOURCE_ID", ""))
        self.smtp_proxy_host = os.getenv("SMTP_PROXY_HOST", "0.0.0.0")
        self.smtp_proxy_port = int(os.getenv("SMTP_PROXY_PORT", "1025"))
        self.mailhog_host = os.getenv("MAILHOG_HOST", "localhost")
        self.mailhog_port = int(os.getenv("MAILHOG_PORT", "1026"))
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.fail_open = self._parse_bool(os.getenv("FAIL_OPEN", "false"))
        self.dataxray_verify_tls = self._parse_bool(
            os.getenv("DATAXRAY_VERIFY_TLS", "true")
        )

    def _parse_bool(self, value: str) -> Optional[bool]:
        """Parse "true" or "false" (any case). Return None for other values."""
        return {"true": True, "false": False}.get(value.strip().lower())
    
    def _parse_datasource_ids(self, datasource_id_str: str) -> List[int]:
        """Parse comma-separated datasource IDs into list of integers."""
        if not datasource_id_str:
            return []
        try:
            return [int(id.strip()) for id in datasource_id_str.split(",")]
        except ValueError:
            return []
    
    def validate(self) -> Optional[str]:
        """Validate configuration and return error message if invalid."""
        if not self.dataxray_api_key:
            return "DATAXRAY_API_KEY environment variable is required"
        
        if not self.dataxray_base_url:
            return "DATAXRAY_BASE_URL environment variable is required"
        
        if not self.datasource_ids:
            return "DATASOURCE_ID environment variable is required (comma-separated list of integers)"

        if self.fail_open is None:
            return "FAIL_OPEN must be true or false"

        if self.dataxray_verify_tls is None:
            return "DATAXRAY_VERIFY_TLS must be true or false"
        
        return None


# Global config instance
config = Config()

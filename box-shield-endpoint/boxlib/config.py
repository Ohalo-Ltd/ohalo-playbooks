import json
import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class Settings:
    # DXR
    dxr_app_url: str
    dxr_pat_token: str

    # Box auth
    box_developer_token: Optional[str] = None
    box_config_json_path: Optional[str] = None
    box_enterprise_id: Optional[str] = None

    # Behavior
    poll_seconds: int = 60
    run_once: bool = True
    http_timeout_seconds: int = 20
    dxr_tags_limit: Optional[int] = None

    # Mapping
    dxr_to_box_classifications: Dict[str, str] = None  # type: ignore


class Config:
    def __init__(self) -> None:
        # Load .env if present in project folder
        load_dotenv(os.path.join(os.getcwd(), ".env"))
        self.settings = self._load_settings()
        self.session = self._build_session()
        self.logger = self._build_logger()

    def _load_settings(self) -> Settings:
        mapping_json = os.getenv("DXR_TO_BOX_CLASSIFICATIONS_JSON", "{}").strip()
        try:
            mapping = json.loads(mapping_json) if mapping_json else {}
        except json.JSONDecodeError:
            mapping = {}

        run_once = os.getenv("RUN_ONCE", "1") in ("1", "true", "True")
        dxr_limit = os.getenv("DXR_TAGS_LIMIT")
        return Settings(
            dxr_app_url=os.environ.get("DXR_APP_URL", ""),
            dxr_pat_token=os.environ.get("DXR_PAT_TOKEN", ""),
            box_developer_token=os.getenv("BOX_DEVELOPER_TOKEN"),
            box_config_json_path=os.getenv("BOX_CONFIG_JSON_PATH"),
            box_enterprise_id=os.getenv("BOX_ENTERPRISE_ID"),
            poll_seconds=int(os.getenv("POLL_SECONDS", "60")),
            run_once=run_once,
            http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
            dxr_tags_limit=int(dxr_limit) if dxr_limit else None,
            dxr_to_box_classifications=mapping or {},
        )

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE"),
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _build_logger(self) -> logging.Logger:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, level, logging.INFO),
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        return logging.getLogger("box-shield-endpoint")


def require_env(cfg: Config) -> None:
    s = cfg.settings
    missing = []
    if not s.dxr_app_url:
        missing.append("DXR_APP_URL")
    if not s.dxr_pat_token:
        missing.append("DXR_PAT_TOKEN")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")



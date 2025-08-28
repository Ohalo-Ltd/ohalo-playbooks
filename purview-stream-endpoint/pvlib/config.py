import os
import logging
from typing import Dict

from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests

# Load env from CWD and from module folder
try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
    load_dotenv()  # cwd
    load_dotenv(os.path.join(os.path.dirname(_HERE), ".env"))  # purview-stream-endpoint/.env
except Exception:
    pass

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("purview-dxr")

HTTP_TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))

def build_http_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

HTTP = build_http_session()

def normalize_base_url(raw: str) -> str:
    if not raw:
        raise SystemExit("DXR_APP_URL is required.")
    url = raw.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")

def normalize_purview_endpoint(raw: str) -> str:
    if not raw:
        raise SystemExit("PURVIEW_ENDPOINT is required.")
    ep = raw.strip()
    if not ep.startswith("http"):
        ep = "https://" + ep
    ep = ep.rstrip("/")
    host = ep.split("//", 1)[1]
    if host == "purview.azure.net":
        raise SystemExit(
            "PURVIEW_ENDPOINT must be your account endpoint (e.g., https://<account>.purview.azure.com), not purview.azure.net"
        )
    if host.endswith("purview.azure.com") and host.count(".") < 3:
        raise SystemExit(
            "PURVIEW_ENDPOINT looks incomplete. Expected format: https://<account>.purview.azure.com"
        )
    return ep

def dxr_env() -> Dict[str, str]:
    base = normalize_base_url(os.getenv("DXR_APP_URL"))
    return {
        "DXR_APP_URL": base,
        "DXR_PAT_TOKEN": os.getenv("DXR_PAT_TOKEN", ""),
        # New vbeta endpoints
        "DXR_CLASSIFICATIONS_PATH": os.getenv("DXR_CLASSIFICATIONS_PATH", "/api/vbeta/classifications"),
        "DXR_FILES_PATH": os.getenv("DXR_FILES_PATH", "/api/vbeta/files"),
        "DXR_VERIFY_SSL": os.getenv("DXR_VERIFY_SSL", "1"),
        "DXR_TENANT": os.getenv("DXR_TENANT", "default"),
    }

def purview_env() -> Dict[str, str]:
    return {
        "PURVIEW_ENDPOINT": normalize_purview_endpoint(os.getenv("PURVIEW_ENDPOINT")),
        "AZURE_TENANT_ID": os.getenv("AZURE_TENANT_ID", ""),
        "AZURE_CLIENT_ID": os.getenv("AZURE_CLIENT_ID", ""),
        "AZURE_CLIENT_SECRET": os.getenv("AZURE_CLIENT_SECRET", ""),
    }

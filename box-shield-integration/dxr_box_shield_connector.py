#!/usr/bin/env python3
"""DXR to Box Shield connector prototype using Box SDK JWT authentication.

Enumerates files from Data X-Ray, fetches associated labels, and applies matching
classifications to Box Shield. Supports a dry-run mode (using local fixtures) and
writes human-readable output to a `.log` file in the configured directory.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib import error, parse, request

try:  # Box SDK is optional for dry-run mode
    from boxsdk import Client, JWTAuth
    from boxsdk.exception import BoxAPIException
    BOX_SDK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - dependency warning path
    BOX_SDK_AVAILABLE = False

    class BoxAPIException(Exception):  # type: ignore[override]
        """Fallback exception to keep type checking happy when SDK is absent."""

    Client = JWTAuth = None  # type: ignore[assignment]

import yaml

APP_NAME = "dxr-box-shield-connector"
CLASSIFICATION_FIELD = "Box__Security__Classification__Key"
BOX_DEFAULT_OAUTH_URL = "https://api.box.com/oauth2/token"


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


@dataclass
class SyncConfig:
    base_url: str
    template_key: str
    box_oauth_url: str
    box_jwt_config_path: Optional[Path]
    dxr_base_url: str
    dxr_api_token: str
    dxr_datasource_id: str
    dxr_files_endpoint: str
    dxr_labels_endpoint: str
    dxr_page_size: int
    label_mapping: Dict[str, str]
    output_dir: Path
    batch_size: int
    retry_attempts: int
    dry_run_files_fixture: Optional[Path]
    dry_run_labels_fixture: Optional[Path]
    log_file_name: str
    log_level: str

    @staticmethod
    def from_yaml(config_path: Path) -> "SyncConfig":
        raw = load_yaml(config_path)
        try:
            box = raw["box"]
            dxr = raw["dxr"]
            labels = raw["labels"]
            sync = raw["sync"]
        except KeyError as exc:
            raise ValueError(f"Missing required config section: {exc}") from exc

        mapping = labels.get("mapping") or {}
        if not mapping:
            raise ValueError("labels.mapping must contain at least one DXR→Box mapping")

        base_dir = config_path.parent
        fixtures = raw.get("dry_run_fixture", {}) or {}
        logging_cfg = raw.get("logging", {}) or {}

        def resolve_path(path_value: Optional[str]) -> Optional[Path]:
            if not path_value:
                return None
            candidate = Path(str(path_value).strip())
            if not candidate.is_absolute():
                candidate = (base_dir / candidate).resolve()
            return candidate

        output_raw = Path(str(sync.get("output_dir", "./output")).strip() or ".")
        if not output_raw.is_absolute():
            output_raw = (base_dir / output_raw).resolve()

        jwt_config_path = resolve_path(box.get("jwt_config_file"))
        if not jwt_config_path:
            raise ValueError("Box JWT configuration file path must be provided via `box.jwt_config_file`")

        return SyncConfig(
            base_url=str(box.get("base_url", "https://api.box.com/2.0")).rstrip("/"),
            template_key=str(box.get("classification_template_key", "securityClassification-6VMVochwUWo")),
            box_oauth_url=str(box.get("oauth_url", BOX_DEFAULT_OAUTH_URL)).strip() or BOX_DEFAULT_OAUTH_URL,
            box_jwt_config_path=jwt_config_path,
            dxr_base_url=str(dxr.get("base_url", "")).rstrip("/"),
            dxr_api_token=str(dxr.get("api_token", "")).strip(),
            dxr_datasource_id=str(dxr.get("datasource_id", "")).strip(),
            dxr_files_endpoint=str(dxr.get("files_endpoint", "")).strip(),
            dxr_labels_endpoint=str(dxr.get("labels_endpoint", "")).strip(),
            dxr_page_size=int(dxr.get("page_size", 500)),
            label_mapping={str(k): str(v) for k, v in mapping.items()},
            output_dir=output_raw,
            batch_size=int(sync.get("batch_size", 50)),
            retry_attempts=int(sync.get("retry_attempts", 3)),
            dry_run_files_fixture=resolve_path(fixtures.get("files")),
            dry_run_labels_fixture=resolve_path(fixtures.get("labels")),
            log_file_name=str(logging_cfg.get("file_name", "sync_run.log")).strip() or "sync_run.log",
            log_level=str(logging_cfg.get("level", "INFO")).strip() or "INFO",
        )


class DXRClient:
    def __init__(self, config: SyncConfig, *, dry_run: bool = False) -> None:
        self.config = config
        self.dry_run = dry_run

    def _load_fixture(self, path: Optional[Path]) -> Dict[str, Any]:
        if not path:
            raise ValueError("Required fixture path missing for dry-run mode")
        if not path.exists():
            raise FileNotFoundError(f"Fixture not found: {path}")
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _build_files_url(self, cursor: Optional[str]) -> str:
        if not self.config.dxr_base_url or not self.config.dxr_files_endpoint:
            raise ValueError("DXR files endpoint must be configured")
        endpoint = self.config.dxr_files_endpoint.format(
            datasource_id=self.config.dxr_datasource_id
        )
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        params: Dict[str, Any] = {"pageSize": self.config.dxr_page_size}
        if cursor:
            params["cursor"] = cursor
        return f"{self.config.dxr_base_url}{endpoint}?{parse.urlencode(params)}"

    def _build_labels_url(self, file_id: str) -> str:
        if not self.config.dxr_base_url or not self.config.dxr_labels_endpoint:
            raise ValueError("DXR labels endpoint must be configured")
        endpoint = self.config.dxr_labels_endpoint.format(file_id=file_id)
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        return f"{self.config.dxr_base_url}{endpoint}"

    def _fetch_json(self, url: str) -> Dict[str, Any]:
        headers = {
            "User-Agent": f"{APP_NAME}/1.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.config.dxr_api_token}",
        }
        req = request.Request(url, headers=headers)
        with request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def enumerate_files(self) -> List[Dict[str, Any]]:
        if self.dry_run:
            payload = self._load_fixture(self.config.dry_run_files_fixture)
            return payload.get("items", [])

        if not self.config.dxr_api_token:
            raise ValueError("DXR API token is required outside dry-run mode")

        results: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            url = self._build_files_url(cursor)
            payload = self._fetch_json(url)
            results.extend(payload.get("items", []))
            page_info = payload.get("page_info") or {}
            if not page_info.get("has_more"):
                break
            cursor = page_info.get("next_cursor")
            if not cursor:
                break
        return results

    def fetch_labels(self, file_ids: Iterable[str]) -> Dict[str, List[Dict[str, Any]]]:
        if self.dry_run:
            fixture = self._load_fixture(self.config.dry_run_labels_fixture)
            return {fid: fixture.get(fid, []) for fid in file_ids}

        label_map: Dict[str, List[Dict[str, Any]]] = {}
        for fid in file_ids:
            url = self._build_labels_url(fid)
            payload = self._fetch_json(url)
            label_map[fid] = payload.get("labels", [])
        return label_map


class BoxJWTAuth:
    def __init__(self, config: SyncConfig, *, dry_run: bool = False) -> None:
        self.config = config
        self.dry_run = dry_run

    def get_client(self) -> Optional[Client]:
        if self.dry_run:
            return None

        if not BOX_SDK_AVAILABLE:  # pragma: no cover - dependency warning path
            raise RuntimeError(
                "boxsdk is required for non-dry-run execution. Install via `pip install boxsdk[jwt]`."
            )

        jwt_config_path = self.config.box_jwt_config_path
        if not jwt_config_path:
            raise ValueError("Box JWT configuration file path must be configured")
        if not jwt_config_path.exists():
            raise FileNotFoundError(f"Box JWT configuration file not found: {jwt_config_path}")

        auth = JWTAuth.from_settings_file(str(jwt_config_path))
        auth.authenticate_instance()
        return Client(auth)


class BoxShieldClient:
    def __init__(self, config: SyncConfig, client: Optional[Client], *, dry_run: bool = False) -> None:
        self.config = config
        self.client = client
        self.dry_run = dry_run

        if not dry_run and not BOX_SDK_AVAILABLE:  # pragma: no cover - dependency warning path
            raise RuntimeError("boxsdk must be installed for live execution")
        if not dry_run and client is None:
            raise ValueError("Box SDK client was not initialised")

    def apply_classification(self, item_id: str, classification_key: str) -> Dict[str, Any]:
        if self.dry_run:
            logging.info(
                "DRY-RUN Box update | item=%s classification=%s",
                item_id,
                classification_key,
            )
            return {"status": "dry_run", "http_status": None}

        file_item = self.client.file(item_id)  # type: ignore[union-attr]
        try:
            file_item.add_classification(classification_key)
            return {"status": "success", "http_status": 200}
        except BoxAPIException as exc:
            status = getattr(exc, "status", getattr(exc, "status_code", None))
            if status == 409:
                file_item.update_classification(classification_key)
                return {"status": "updated", "http_status": status}
            logging.error("Box classification failed item=%s status=%s error=%s", item_id, status, exc)
            return {"status": "failed", "http_status": status}


def resolve_classification(
    dxr_labels: List[Dict[str, Any]],
    label_mapping: Dict[str, str],
) -> Optional[Dict[str, str]]:
    for label in dxr_labels:
        key = label.get("label_key")
        mapped = label_mapping.get(str(key)) if key else None
        if mapped:
            return {
                "dxr_label_key": str(key),
                "dxr_label_name": label.get("label_name"),
                "box_classification_key": mapped,
            }
    return None


def run_connector(config: SyncConfig, *, dry_run: bool) -> None:
    dxr_client = DXRClient(config, dry_run=dry_run)
    files = dxr_client.enumerate_files()
    logging.info("Enumerated %d files from DXR", len(files))

    file_ids = [entry.get("file_id") or entry.get("item_id") for entry in files]
    file_ids = [fid for fid in file_ids if fid]
    labels_map = dxr_client.fetch_labels(file_ids)
    logging.info("Retrieved labels for %d files", len(labels_map))

    box_client_instance = BoxJWTAuth(config, dry_run=dry_run).get_client()
    box_client = BoxShieldClient(config, box_client_instance, dry_run=dry_run)

    processed = classified = skipped = failed = 0

    for entry in files:
        item_id = entry.get("file_id") or entry.get("item_id")
        item_name = entry.get("file_name") or entry.get("item_name")
        if not item_id:
            logging.warning("Skipping file with missing ID: %s", entry)
            skipped += 1
            continue

        dxr_labels = labels_map.get(item_id, [])
        classification = resolve_classification(dxr_labels, config.label_mapping)
        if not classification:
            logging.info("No mapped classification for %s (%s); skipping", item_id, item_name)
            skipped += 1
            continue

        result = box_client.apply_classification(item_id, classification["box_classification_key"])
        status = result.get("status")
        processed += 1

        if status in {"success", "updated", "dry_run"}:
            classified += 1
            logging.info(
                "Applied classification %s to %s (%s) [status=%s]",
                classification["box_classification_key"],
                item_id,
                item_name,
                status,
            )
        else:
            failed += 1
            logging.error(
                "Failed to classify %s (%s); status=%s",
                item_id,
                item_name,
                status,
            )

    logging.info(
        "Processing complete | processed=%d classified=%d skipped=%d failed=%d",
        processed,
        classified,
        skipped,
        failed,
    )


def configure_logging(config: SyncConfig, *, override_level: Optional[str]) -> Path:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    log_path = config.output_dir / config.log_file_name

    level_name = override_level or config.log_level or "INFO"
    level = getattr(logging, level_name.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        ],
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    return log_path


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DXR to Box Shield connector")
    parser.add_argument("--config", required=True, help="Path to YAML configuration file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Use local fixtures instead of calling DXR/Box APIs",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Override log level (INFO, DEBUG, ...)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    config_path = Path(args.config).expanduser().resolve()
    if not config_path.exists():
        logging.basicConfig(level=logging.ERROR)
        logging.error("Configuration file not found: %s", config_path)
        return 1

    try:
        config = SyncConfig.from_yaml(config_path)
    except Exception as exc:  # noqa: BLE001
        logging.basicConfig(level=logging.ERROR)
        logging.error("Failed to parse configuration: %s", exc)
        return 1

    log_path = configure_logging(config, override_level=args.log_level)
    logging.info("Starting DXR → Box Shield sync | config=%s", config_path)
    logging.info("Log file: %s", log_path)

    if not args.dry_run:
        missing = []
        if not config.box_jwt_config_path:
            missing.append("box.jwt_config_file")
        if not config.dxr_api_token:
            missing.append("dxr.api_token")
        if missing:
            logging.error("Missing required configuration values: %s", ", ".join(missing))
            return 1
        if not config.box_jwt_config_path.exists():
            logging.error("Box JWT configuration file not found: %s", config.box_jwt_config_path)
            return 1

    try:
        run_connector(config, dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Connector execution failed: %s", exc)
        return 1

    logging.info("DXR → Box Shield sync completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

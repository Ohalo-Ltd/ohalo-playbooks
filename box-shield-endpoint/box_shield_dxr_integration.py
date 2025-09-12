"""
DXR → Box Shield integration (skeleton)
- Fetch DXR classifications
- Map to Box Security Classifications
- Apply to target files (placeholder: requires Box file IDs association)
"""
import os
import time
from typing import Any, Dict, List

from boxlib.config import Config, require_env
from boxlib.dxr import fetch_classifications
from boxlib.box_client import BoxClientProvider
from boxlib.mapping import map_dxr_label_to_classification
from boxlib import shield


def _resolve_target_file_id(label: Dict[str, Any]) -> str:
    """
    Placeholder to resolve the Box file ID for a given DXR label/classification record.
    In a real implementation, this would come from DXR file hits (e.g., an externalId or mapping).
    """
    return str(label.get("box_file_id", ""))  # empty -> skip


def run_once(cfg: Config) -> None:
    s = cfg.settings
    log = cfg.logger
    require_env(cfg)

    # Auth to Box
    box = BoxClientProvider(cfg.session, timeout=s.http_timeout_seconds)
    token = box.get_bearer_token(
        developer_token=s.box_developer_token,
        config_json_path=s.box_config_json_path,
        enterprise_id=s.box_enterprise_id,
    )

    # Fetch DXR labels
    labels: List[Dict[str, Any]] = fetch_classifications(
        cfg.session, s.dxr_app_url, s.dxr_pat_token, s.dxr_tags_limit
    )
    log.info("Fetched %d DXR labels", len(labels))

    # Map + apply
    updated = 0
    skipped = 0
    for lab in labels:
        file_id = _resolve_target_file_id(lab)
        if not file_id:
            skipped += 1
            continue
        classification = map_dxr_label_to_classification(s.dxr_to_box_classifications, lab)
        if not classification:
            skipped += 1
            continue
        try:
            current = shield.get_current_classification(cfg.session, token, file_id, s.http_timeout_seconds)
            if current == classification:
                continue
            shield.apply_classification(cfg.session, token, file_id, classification, s.http_timeout_seconds)
            updated += 1
            log.info("Applied classification '%s' to file %s (was: %s)", classification, file_id, current)
        except Exception as e:
            log.error("Failed to classify file %s: %s", file_id, e)

    log.info("Done. updated=%d skipped=%d", updated, skipped)


def main() -> None:
    cfg = Config()
    if cfg.settings.run_once:
        run_once(cfg)
        return
    # Polling mode
    while True:
        run_once(cfg)
        time.sleep(cfg.settings.poll_seconds)


if __name__ == "__main__":
    main()

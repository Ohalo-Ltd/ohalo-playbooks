from typing import Any, Dict, Optional

import requests

TEMPLATE_KEY = "securityClassification-6VMVochwUWo"  # Box Security Classification template key


def get_current_classification(
    session: requests.Session, token: str, file_id: str, timeout: int = 20
) -> Optional[str]:
    url = f"https://api.box.com/2.0/files/{file_id}/metadata/enterprise/{TEMPLATE_KEY}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = session.get(url, headers=headers, timeout=timeout)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    return data.get("Box__Security__Classification__Key")


def apply_classification(
    session: requests.Session,
    token: str,
    file_id: str,
    classification_key: str,
    timeout: int = 20,
) -> None:
    """
    Upsert the Security Classification metadata on a file. If metadata exists, update; else, create.
    """
    base_url = f"https://api.box.com/2.0/files/{file_id}/metadata/enterprise/{TEMPLATE_KEY}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Try update first (PATCH)
    patch_body = [
        {
            "op": "replace",
            "path": "/Box__Security__Classification__Key",
            "value": classification_key,
        }
    ]
    resp = session.put(base_url, headers=headers, json={"Box__Security__Classification__Key": classification_key}, timeout=timeout)
    if resp.status_code in (200, 201):
        return
    if resp.status_code == 409:
        # Existing metadata, fall back to update via PATCH
        patch_url = base_url
        patch_headers = headers.copy()
        patch_headers["Content-Type"] = "application/json-patch+json"
        patch_resp = session.request(
            "PATCH", patch_url, headers=patch_headers, json=patch_body, timeout=timeout
        )
        patch_resp.raise_for_status()
        return
    # If metadata missing, create
    if resp.status_code == 404:
        create_resp = session.post(
            base_url,
            headers=headers,
            json={"Box__Security__Classification__Key": classification_key},
            timeout=timeout,
        )
        create_resp.raise_for_status()
        return
    resp.raise_for_status()


# Placeholders for future Shield Information Barrier helpers

def list_information_barrier_segments(session: requests.Session, token: str, limit: int = 100, timeout: int = 20) -> Dict[str, Any]:
    url = f"https://api.box.com/2.0/shield_information_barrier_segments?limit={limit}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    resp = session.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


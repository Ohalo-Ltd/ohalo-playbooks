import json
import os
from typing import Any, Dict, Optional

import requests

try:
    from boxsdk import Client
    from boxsdk.oauth2 import JWTAuth, OAuth2
except Exception:  # pragma: no cover - SDK optional at import time
    Client = object  # type: ignore
    JWTAuth = object  # type: ignore
    OAuth2 = object  # type: ignore


class BoxClientProvider:
    def __init__(self, session: requests.Session, timeout: int = 20) -> None:
        self.session = session
        self.timeout = timeout

    def get_bearer_token(
        self,
        developer_token: Optional[str] = None,
        config_json_path: Optional[str] = None,
        enterprise_id: Optional[str] = None,
    ) -> str:
        """
        Returns a bearer token for REST calls.
        - If developer_token provided, use it (quick-start only).
        - Else if JWT config provided, mint a JWT service token.
        """
        if developer_token:
            return developer_token

        if config_json_path:
            if JWTAuth is object:
                raise RuntimeError(
                    "boxsdk not available; install requirements.txt for JWT auth"
                )
            with open(config_json_path, "r", encoding="utf-8") as f:
                jwt_config = json.load(f)
            auth = JWTAuth.from_settings_dictionary(jwt_config)
            if enterprise_id:
                auth.authenticate_instance()
                access_token = auth._access_token  # type: ignore[attr-defined]
                if not access_token:
                    access_token = auth.refresh(None)
                return auth.access_token  # type: ignore[return-value]
            # Fallback: service account token
            return auth.authenticate_instance()  # type: ignore[return-value]

        raise RuntimeError(
            "No Box auth configured. Provide BOX_DEVELOPER_TOKEN or BOX_CONFIG_JSON_PATH."
        )

    def rest_get(self, token: str, url: str, accept: str = "application/json") -> requests.Response:
        headers = {"Authorization": f"Bearer {token}", "Accept": accept}
        resp = self.session.get(url, headers=headers, timeout=self.timeout)
        return resp

    def rest_put(self, token: str, url: str, json_body: Dict[str, Any]) -> requests.Response:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = self.session.put(url, headers=headers, json=json_body, timeout=self.timeout)
        return resp



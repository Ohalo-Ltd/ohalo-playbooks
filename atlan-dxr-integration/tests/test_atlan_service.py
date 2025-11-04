"""Tests for the Atlan service compatibility layer."""

from __future__ import annotations

from typing import Any, List

import pytest

from atlan_dxr_integration import atlan_service


class _StubAsset:
    def __init__(self, payload: dict[str, Any]):
        self._payload = payload

    def dict(self, *, by_alias: bool, exclude_none: bool):
        return self._payload


class _StubResults:
    def __init__(self, entities: List[_StubAsset]):
        self._entities = entities
        self.current_page_called = False

    @property
    def count(self) -> int:
        return 3

    def current_page(self):
        self.current_page_called = True
        return self._entities


class _StubAssetClient:
    def __init__(self, results: _StubResults):
        self._results = results
        self.search_calls: list[Any] = []

    def search(self, request):
        self.search_calls.append(request)
        return self._results


class _StubAtlanClient:
    def __init__(self, asset_client: _StubAssetClient):
        self.asset = asset_client


def test_search_assets_uses_property_count(monkeypatch: pytest.MonkeyPatch) -> None:
    results = _StubResults(entities=[_StubAsset({"guid": "123"})])
    asset_client = _StubAssetClient(results)
    stub_client = _StubAtlanClient(asset_client)

    monkeypatch.setattr(
        atlan_service,
        "get_atlan_client",
        lambda base_url, api_key: stub_client,
    )

    client = atlan_service.AtlanRESTClient(
        base_url="https://atlan.example.com",
        api_key="token",
    )

    payload = {
        "dsl": {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "Connection"}},
                    ]
                }
            },
            "size": 1,
        }
    }

    response = client.search_assets(payload)

    assert response["entities"] == [{"guid": "123"}]
    assert response["approximateCount"] == 3
    assert results.current_page_called is True
    assert len(asset_client.search_calls) == 1

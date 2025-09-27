"""Unit tests for the DXR HTTP client."""

from __future__ import annotations

import json

import pytest

from atlan_dxr_integration.dxr_client import DXRClient


class _FakeResponse:
    def __init__(self, lines):
        self._lines = list(lines)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    def iter_lines(self, decode_unicode=True):
        for line in self._lines:
            yield line


class _FakeSession:
    def __init__(self, response: _FakeResponse):
        self._response = response
        self.headers = {}
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return self._response

    def close(self):
        return None


def test_stream_files_returns_file_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    sample_payload = {
        "fileId": "0KQUMpgBVo-c9i0dUnJ8",
        "fileName": "2011_audited_financial_statement_msword.doc",
        "path": "Documents/Confidential Folder/2011_audited_financial_statement_msword.doc",
        "size": 1_048_576,
    }
    response = _FakeResponse([json.dumps(sample_payload)])

    fake_session = _FakeSession(response)

    def _fake_session_factory():
        return fake_session

    monkeypatch.setattr("atlan_dxr_integration.dxr_client.requests.Session", _fake_session_factory)

    client = DXRClient("https://dxr.example.com/api", "token")
    files = list(client.stream_files())

    assert fake_session.requests[0][0].endswith("/vbeta/files")
    assert files == [sample_payload]


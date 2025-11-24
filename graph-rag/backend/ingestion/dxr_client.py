"""DXR (Data X-Ray) client for fetching documents and metadata."""

import asyncio
from typing import Any, Optional

import httpx
from pydantic import BaseModel, Field

from core.config import settings


class DXRFile(BaseModel):
    """DXR file model."""

    id: str
    name: str
    path: str
    size: int
    mime_type: str = Field(alias="mimeType")
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
    categories: list[str] = []
    entitlements: list[str] = []
    extracted_metadata: dict[str, Any] = Field(default_factory=dict, alias="extractedMetadata")
    content: Optional[str] = None

    class Config:
        populate_by_name = True


class DXRClient:
    """Client for interacting with Data X-Ray API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        """Initialize DXR client.

        Args:
            api_key: DXR API key (defaults to settings)
            base_url: Base URL for DXR API (defaults to settings)
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.api_key = api_key or settings.dxr_api_key
        self.base_url = (base_url or settings.dxr_base_url).rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

        if not self.api_key:
            raise ValueError("DXR API key is required")

        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    async def __aenter__(self) -> "DXRClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def _request_with_retry(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> httpx.Response:
        """Make HTTP request with exponential backoff retry.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            **kwargs: Additional arguments for httpx request

        Returns:
            HTTP response

        Raises:
            httpx.HTTPError: If all retries fail
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = await self.client.request(method, endpoint, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s
                    wait_time = 2**attempt
                    await asyncio.sleep(wait_time)
                    continue
                break

        raise last_error or httpx.HTTPError("Request failed")

    async def fetch_documents(
        self,
        datasource_id: str,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[dict[str, Any]] = None,
    ) -> list[DXRFile]:
        """Fetch documents from a DXR datasource.

        Args:
            datasource_id: DXR datasource ID
            limit: Maximum number of documents to fetch
            offset: Offset for pagination
            filters: Optional filters (e.g., {"category": ["Contract"]})

        Returns:
            List of DXR files
        """
        params: dict[str, Any] = {
            "datasourceId": datasource_id,
            "limit": limit,
            "offset": offset,
        }

        if filters:
            params.update(filters)

        response = await self._request_with_retry("GET", "/api/v1/files", params=params)
        data = response.json()

        return [DXRFile(**file_data) for file_data in data.get("files", [])]

    async def fetch_document_content(self, file_id: str) -> str:
        """Fetch document content.

        Args:
            file_id: DXR file ID

        Returns:
            Document content as text
        """
        response = await self._request_with_retry("GET", f"/api/v1/files/{file_id}/content")
        return response.text

    async def fetch_all_documents(
        self,
        datasource_id: str,
        batch_size: int = 100,
        max_documents: Optional[int] = None,
        filters: Optional[dict[str, Any]] = None,
    ) -> list[DXRFile]:
        """Fetch all documents from a datasource with pagination.

        Args:
            datasource_id: DXR datasource ID
            batch_size: Number of documents per batch
            max_documents: Maximum total documents to fetch (None for all)
            filters: Optional filters

        Returns:
            List of all DXR files
        """
        all_files: list[DXRFile] = []
        offset = 0

        while True:
            files = await self.fetch_documents(
                datasource_id=datasource_id,
                limit=batch_size,
                offset=offset,
                filters=filters,
            )

            if not files:
                break

            all_files.extend(files)
            offset += len(files)

            if max_documents and len(all_files) >= max_documents:
                all_files = all_files[:max_documents]
                break

            # Stop if we got fewer files than requested (last page)
            if len(files) < batch_size:
                break

        return all_files

"""PostgreSQL database client."""

from typing import Any, Optional

import asyncpg
from asyncpg import Pool

from core.config import settings


class PostgresClient:
    """Client for PostgreSQL database."""

    def __init__(self, connection_url: Optional[str] = None):
        """Initialize Postgres client.

        Args:
            connection_url: PostgreSQL connection URL
        """
        self.connection_url = connection_url or settings.postgres_url
        self.pool: Optional[Pool] = None

    async def __aenter__(self) -> "PostgresClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def connect(self) -> None:
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(
            self.connection_url,
            min_size=2,
            max_size=10,
        )

    async def close(self) -> None:
        """Close connection pool."""
        if self.pool:
            await self.pool.close()

    async def execute(
        self, query: str, *args: Any, timeout: float = 10.0
    ) -> str:
        """Execute a query that doesn't return results.

        Args:
            query: SQL query
            *args: Query parameters
            timeout: Query timeout in seconds

        Returns:
            Query status
        """
        if not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")

        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args, timeout=timeout)

    async def fetch(
        self, query: str, *args: Any, timeout: float = 10.0
    ) -> list[asyncpg.Record]:
        """Fetch multiple rows.

        Args:
            query: SQL query
            *args: Query parameters
            timeout: Query timeout in seconds

        Returns:
            List of records
        """
        if not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")

        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args, timeout=timeout)

    async def fetchrow(
        self, query: str, *args: Any, timeout: float = 10.0
    ) -> Optional[asyncpg.Record]:
        """Fetch a single row.

        Args:
            query: SQL query
            *args: Query parameters
            timeout: Query timeout in seconds

        Returns:
            Record or None
        """
        if not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")

        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args, timeout=timeout)

    async def fetchval(
        self, query: str, *args: Any, timeout: float = 10.0
    ) -> Any:
        """Fetch a single value.

        Args:
            query: SQL query
            *args: Query parameters
            timeout: Query timeout in seconds

        Returns:
            Single value
        """
        if not self.pool:
            raise RuntimeError("Not connected to PostgreSQL")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args, timeout=timeout)

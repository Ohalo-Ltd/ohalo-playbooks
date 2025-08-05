"""
Database manager for ingestion operations (uses superuser privileges)
"""

from contextlib import contextmanager
from typing import Generator
import psycopg2
from urllib.parse import urlparse

from .config import IngestionSettings


class IngestionDatabaseManager:
    """Database manager for ingestion operations - uses superuser privileges"""

    def __init__(self, settings: IngestionSettings | None = None):
        self.settings = settings or IngestionSettings()

    def _get_connection_params(self) -> dict:
        """Parse database URL into connection parameters"""
        parsed = urlparse(self.settings.database_url)
        return {
            "host": parsed.hostname,
            "port": parsed.port,
            "database": parsed.path[1:],  # Remove leading '/'
            "user": parsed.username,
            "password": parsed.password,
        }

    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager for database connections"""
        connection = None
        try:
            connection = psycopg2.connect(**self._get_connection_params())
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            if connection:
                connection.close()

    def check_table_exists(self, table_name: str = "documents") -> bool:
        """Check if a table exists"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        );
                    """, (table_name,))
                    result = cursor.fetchone()
                    return result[0] if result else False
        except Exception:
            return False

    def check_documents_exist(self) -> bool:
        """Check if documents table has data"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT COUNT(*) FROM documents')
                    result = cursor.fetchone()
                    return result[0] > 0 if result else False
        except Exception:
            return False

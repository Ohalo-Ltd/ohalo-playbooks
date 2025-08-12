"""
Database manager for application operations with Row Level Security (RLS)
Uses non-privileged user for secure database access
"""

from contextlib import contextmanager
from typing import List, Dict, Any, Generator
import psycopg2
from urllib.parse import urlparse

from core.config import Settings
from core.logging import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """Database manager for RAG operations - uses non-privileged user with RLS"""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()

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

    def set_rls_context(self, connection, user_info: Dict[str, Any]):
        """Set Row Level Security context variables (user employee_id is set for RLS)"""
        with connection.cursor() as cursor:
            email = user_info.get("email")
            if not email:
                raise ValueError("User email is required for RLS context")
            employee_id = user_info.get("employee_id", "")
            department = user_info.get("department", "")
            groups = user_info.get("groups", [])
            groups_str = ",".join(groups) if groups else ""
            # Set RLS context variables (user employee_id is set)
            logger.debug(f"[RLS DEBUG] SQL: SET app.user_email = {email}")
            cursor.execute(f"SET app.user_email = %s", (email,))
            logger.debug(f"[RLS DEBUG] SQL: SET app.employee_id = {employee_id}")
            cursor.execute(f"SET app.employee_id = %s", (employee_id,))
            logger.debug(f"[RLS DEBUG] SQL: SET app.department = {department}")
            cursor.execute(f"SET app.department = %s", (department,))
            logger.debug(f"[RLS DEBUG] SQL: SET app.groups = {groups_str}")
            cursor.execute(f"SET app.groups = %s", (groups_str,))
            connection.commit()
            # Debug: print RLS context after setting
            cursor.execute("SELECT current_setting('app.user_email', true), current_setting('app.employee_id', true), current_setting('app.groups', true)")
            rls_ctx = cursor.fetchone()
            logger.debug(f"[RLS DEBUG] After set_rls_context: user_email={rls_ctx[0]}, employee_id={rls_ctx[1]}, groups={rls_ctx[2]}")

    def search_similar_chunks(
        self, 
        connection, 
        query_embedding: List[float], 
        limit: int = 5, 
        similarity_threshold: float = 0.5
    ) -> List[Dict[str, Any]]:
        """Search for similar document chunks using vector similarity"""
        with connection.cursor() as cursor:
            sql = """
                SELECT 
                    dc.id,
                    dc.content,
                    dc.chunk_index,
                    d.title,
                    d.category,
                    d.extracted_employee_id,
                    1 - (dc.embedding <=> %s::vector) as similarity
                FROM document_chunks dc
                JOIN documents d ON dc.document_id = d.id
                WHERE 1 - (dc.embedding <=> %s::vector) > %s
                ORDER BY dc.embedding <=> %s::vector
                LIMIT %s
            """
            # Print first 5 values of embedding and threshold
            emb_preview = query_embedding[:5] if query_embedding else []
            param_summary = f"query_embedding[:5]: {emb_preview}, similarity_threshold: {similarity_threshold}, limit: {limit}"
            logger.debug(f"[RLS DEBUG] SQL: {sql.strip()} | PARAMS: {param_summary}")
            params = (query_embedding, query_embedding, similarity_threshold, query_embedding, limit)
            cursor.execute(sql, params)
            results = []
            for row in cursor.fetchall():
                results.append({
                    "id": row[0],
                    "content": row[1],
                    "chunk_index": row[2],
                    "title": row[3],
                    "category": row[4],
                    "extracted_employee_id": row[5],
                    "similarity": float(row[6])
                })
            logger.debug(f"[RLS DEBUG] search_similar_chunks returned {len(results)} rows")
            return results

    def search_chunks(self, query_embedding: List[float], user_info: Dict[str, Any], limit: int = 5, similarity_threshold: float = 0.2) -> List[Dict[str, Any]]:
        """Search for similar document chunks with RLS applied and adjustable similarity threshold"""
        with self.get_connection() as conn:
            # Set RLS context first
            self.set_rls_context(conn, user_info)
            # Use the existing search_similar_chunks method
            return self.search_similar_chunks(conn, query_embedding, limit, similarity_threshold)

    def get_document_stats(self, connection) -> Dict[str, int]:
        """Get document statistics visible to current user"""
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM documents")
            result = cursor.fetchone()
            doc_count = result[0] if result else 0
            
            cursor.execute("SELECT COUNT(*) FROM document_chunks")
            result = cursor.fetchone()
            chunk_count = result[0] if result else 0
            
            return {
                "documents": doc_count,
                "chunks": chunk_count
            }

    def check_database_connection(self) -> bool:
        """Check if database connection is working"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return (result[0] if result else 0) == 1
        except Exception:
            return False

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
                        )
                    """, (table_name,))
                    result = cursor.fetchone()
                    return bool(result[0] if result else False)
        except Exception:
            return False

    def get_documents_for_user(self, user_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get documents accessible to a user based on RLS policies (no employee_id)"""
        with self.get_connection() as conn:
            self.set_rls_context(conn, user_info)
            with conn.cursor() as cursor:
                # Debug: print RLS context before query
                cursor.execute("SELECT current_setting('app.user_email', true), current_setting('app.employee_id', true), current_setting('app.groups', true)")
                rls_ctx = cursor.fetchone()
                logger.debug(f"[RLS DEBUG] Before get_documents_for_user query: user_email={rls_ctx[0]}, employee_id={rls_ctx[1]}, groups={rls_ctx[2]}")
                sql = """
                    SELECT id, title, category, extracted_employee_id, created_at
                    FROM documents
                    ORDER BY created_at DESC
                """
                logger.debug(f"[RLS DEBUG] SQL: {sql.strip()}")
                cursor.execute(sql)
                results = []
                for row in cursor.fetchall():
                    results.append({
                        "id": row[0],
                        "title": row[1],
                        "category": row[2],
                        "extracted_employee_id": row[3],
                        "created_at": row[4].isoformat() if row[4] else None
                    })
                logger.debug(f"[RLS DEBUG] get_documents_for_user returned {len(results)} rows")
                return results

    def get_unique_documents_for_user(self, user_info: Dict[str, Any], limit: int = 20) -> List[Dict[str, Any]]:
        """Get unique documents accessible to a user (for document listing queries, no employee_id)"""
        with self.get_connection() as conn:
            self.set_rls_context(conn, user_info)
            with conn.cursor() as cursor:
                # Debug: print RLS context before query
                cursor.execute("SELECT current_setting('app.user_email', true), current_setting('app.employee_id', true), current_setting('app.groups', true)")
                rls_ctx = cursor.fetchone()
                logger.debug(f"[RLS DEBUG] Before get_unique_documents_for_user query: user_email={rls_ctx[0]}, employee_id={rls_ctx[1]}, groups={rls_ctx[2]}")
                sql = """
                    SELECT DISTINCT 
                        d.id,
                        d.title,
                        d.category,
                        d.extracted_employee_id,
                        d.created_at,
                        SUBSTRING(d.content, 1, 300) as content_preview
                    FROM documents d
                    ORDER BY d.created_at DESC
                    LIMIT %s
                """
                logger.debug(f"[RLS DEBUG] SQL: {sql.strip()} | PARAMS: {limit}")
                cursor.execute(sql, (limit,))
                results = []
                for row in cursor.fetchall():
                    results.append({
                        "id": row[0],
                        "title": row[1],
                        "category": row[2],
                        "extracted_employee_id": row[3],
                        "created_at": row[4].isoformat() if row[4] else None,
                        "content": row[5]
                    })
                logger.debug(f"[RLS DEBUG] get_unique_documents_for_user returned {len(results)} rows")
                return results

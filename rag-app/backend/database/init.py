"""
Database initialization and schema management
Handles database setup, migrations, and user creation
"""

from pathlib import Path
from contextlib import contextmanager
from typing import Generator
import psycopg2
from urllib.parse import urlparse

from core.config import Settings
from core.config import yaml_config
from core.logging import get_logger

logger = get_logger(__name__)


class DatabaseInitializer:
    """Handles database initialization and schema setup"""

    def __init__(self, settings: Settings | None = None, selected_policy: str | None = None):
        self.settings = settings or Settings()
        # Use postgres superuser for initialization
        self.admin_url = "postgresql://postgres:password@localhost:5432/ragapp"
        # Optional selected policy path relative to backend/ (e.g. database/policies/hr.sql)
        self.selected_policy = selected_policy
        
    def _get_admin_connection_params(self) -> dict:
        """Get connection params for postgres superuser"""
        parsed = urlparse(self.admin_url)
        return {
            "host": parsed.hostname,
            "port": parsed.port,
            "database": parsed.path[1:],
            "user": parsed.username,
            "password": parsed.password,
        }

    @contextmanager
    def get_admin_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Get admin connection for database initialization"""
        connection = None
        try:
            connection = psycopg2.connect(**self._get_admin_connection_params())
            connection.autocommit = True  # For DDL operations
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            if connection:
                connection.close()

    def initialize_database(self) -> bool:
        """Initialize database schema and users"""
        try:
            logger.info("🗄️  Initializing database schema...")
            
            # Get the directory containing SQL files
            backend_dir = Path(__file__).parent.parent
            
            # Run main schema initialization
            schema_file = backend_dir / "database" / "init" / "schema.sql"
            if schema_file.exists():
                self._execute_sql_file(schema_file)
            else:
                logger.warning(f"⚠️  Schema file not found: {schema_file}")
                return False
            
            # Add dynamic columns based on configuration
            self._add_dynamic_columns()
            
            # Create application user
            user_file = backend_dir / "database" / "init" / "create_user.sql"
            if user_file.exists():
                self._execute_sql_file(user_file)
            else:
                logger.warning(f"⚠️  User creation file not found: {user_file}")
                return False

            # If a selected policy was supplied, try to execute it from backend/<selected_policy>
            if self.selected_policy:
                policy_path = backend_dir / self.selected_policy
                if policy_path.exists():
                    logger.info(f"📄 Applying selected policy: {policy_path.name}")
                    self._execute_sql_file(policy_path)
                else:
                    logger.warning(f"⚠️  Selected policy not found: {policy_path}")

            logger.info("✅ Database initialization completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            return False

    def _execute_sql_file(self, file_path: Path) -> None:
        """Execute SQL commands from a file"""
        logger.info(f"📄 Executing SQL file: {file_path.name}")
        
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        with self.get_admin_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # Execute the entire file as one block to handle DO $$ blocks correctly
                    cursor.execute(sql_content)
                    logger.info(f"✅ Successfully executed {file_path.name}")
                except Exception as e:
                    logger.error(f"❌ Failed to execute {file_path.name}: {e}")
                    # Try to split and execute line by line for better error reporting
                    lines = sql_content.split('\n')
                    current_statement = []
                    in_dollar_quote = False
                    
                    for line in lines:
                        line = line.strip()
                        if not line or line.startswith('--'):
                            continue
                            
                        current_statement.append(line)
                        
                        # Handle DO $$ blocks
                        if 'DO $$' in line:
                            in_dollar_quote = True
                        elif '$$' in line and in_dollar_quote and 'DO $$' not in line:
                            in_dollar_quote = False
                            # Execute the complete DO block
                            stmt = '\n'.join(current_statement)
                            try:
                                cursor.execute(stmt)
                            except Exception as block_error:
                                logger.warning(f"⚠️  SQL block failed: {block_error}")
                            current_statement = []
                        elif not in_dollar_quote and line.endswith(';'):
                            # Regular statement
                            stmt = '\n'.join(current_statement)
                            try:
                                cursor.execute(stmt)
                            except Exception as stmt_error:
                                logger.warning(f"⚠️  SQL statement failed: {stmt_error}")
                            current_statement = []

    def _add_dynamic_columns(self) -> None:
        """Add dynamic columns to documents table based on configuration"""
        try:
            app_config = yaml_config
            columns = app_config.get_metadata_columns_for_database()
            
            if not columns:
                logger.info("📋 No custom metadata columns configured")
                return
            
            logger.info(f"📋 Adding {len(columns)} custom metadata columns...")
            
            with self.get_admin_connection() as conn:
                with conn.cursor() as cursor:
                    for column_name, column_type in columns:
                        # Check if column already exists
                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name = 'documents' 
                                AND column_name = %s
                            )
                        """, (column_name,))
                        
                        exists = cursor.fetchone()[0]
                        
                        if not exists:
                            # Add the column
                            sql = f'ALTER TABLE documents ADD COLUMN "{column_name}" {column_type}'
                            cursor.execute(sql)
                            logger.info(f"  ✅ Added column: {column_name} ({column_type})")
                        else:
                            logger.info(f"  ℹ️  Column already exists: {column_name}")
                            
        except Exception as e:
            logger.error(f"❌ Failed to add dynamic columns: {e}")
            raise

    def check_database_setup(self) -> bool:
        """Check if database is properly set up"""
        try:
            with self.get_admin_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if main tables exist
                    cursor.execute("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name IN ('documents', 'document_chunks')
                    """)
                    result = cursor.fetchone()
                    table_count = result[0] if result else 0
                    
                    # Check if application user exists
                    cursor.execute("""
                        SELECT COUNT(*) FROM pg_catalog.pg_roles 
                        WHERE rolname = 'ragapp_user'
                    """)
                    result = cursor.fetchone()
                    user_exists = (result[0] if result else 0) > 0
                    
                    return table_count == 2 and user_exists
                    
        except Exception as e:
            logger.error(f"❌ Database check failed: {e}")
            return False

    def reset_database(self) -> bool:
        """Reset database by dropping and recreating tables"""
        try:
            logger.info("🗑️  Resetting database...")
            
            with self.get_admin_connection() as conn:
                with conn.cursor() as cursor:
                    # Drop tables in correct order (chunks first due to foreign key)
                    cursor.execute("DROP TABLE IF EXISTS document_chunks CASCADE")
                    cursor.execute("DROP TABLE IF EXISTS documents CASCADE")
                    
            logger.info("✅ Database reset completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database reset failed: {e}")
            return False

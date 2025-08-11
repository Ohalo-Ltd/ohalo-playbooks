"""
Database initialization script
Sets up the database schema and users
"""

import sys
from database import DatabaseInitializer
from core.config import Settings
from core.logging import get_logger

logger = get_logger(__name__)

def main():
    """Initialize the database"""
    
    # Check for --reset flag
    reset_requested = "--reset" in sys.argv
    
    logger.info("🚀 Starting database initialization...")
    
    settings = Settings()
    initializer = DatabaseInitializer(settings)
    
    # Handle reset request
    if reset_requested:
        logger.info("🔄 Reset requested via --reset flag")
        if initializer.reset_database():
            logger.info("🔄 Database reset successful, proceeding with initialization...")
        else:
            logger.error("❌ Database reset failed")
            return False
    else:
        # Check if already initialized
        if initializer.check_database_setup():
            logger.info("✅ Database is already initialized")
            return True
    
    # Initialize database
    success = initializer.initialize_database()
    
    if success:
        logger.info("✅ Database initialization completed successfully!")
        logger.info("💡 You can now run document ingestion with: python ingest_documents.py")
    else:
        logger.error("❌ Database initialization failed")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

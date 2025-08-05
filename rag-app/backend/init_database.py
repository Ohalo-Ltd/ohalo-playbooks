"""
Database initialization script
Sets up the database schema and users
"""

import sys
from database import DatabaseInitializer
from core.config import Settings

def main():
    """Initialize the database"""
    
    # Check for --reset flag
    reset_requested = "--reset" in sys.argv
    
    print("🚀 Starting database initialization...")
    
    settings = Settings()
    initializer = DatabaseInitializer(settings)
    
    # Handle reset request
    if reset_requested:
        print("🔄 Reset requested via --reset flag")
        if initializer.reset_database():
            print("🔄 Database reset successful, proceeding with initialization...")
        else:
            print("❌ Database reset failed")
            return False
    else:
        # Check if already initialized
        if initializer.check_database_setup():
            print("✅ Database is already initialized")
            return True
    
    # Initialize database
    success = initializer.initialize_database()
    
    if success:
        print("✅ Database initialization completed successfully!")
        print("💡 You can now run document ingestion with: python ingest_documents.py")
    else:
        print("❌ Database initialization failed")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

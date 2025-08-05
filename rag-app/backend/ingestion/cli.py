"""
Command-line interface for document ingestion
"""

import sys
from .config import IngestionSettings
from .processor import DocumentProcessor
from .database import IngestionDatabaseManager


def main():
    """Main ingestion function that uses superuser connection - bypasses RLS automatically"""
    settings = IngestionSettings()
    
    # Force superuser connection regardless of environment variables
    settings.database_url = "postgresql://postgres:password@localhost:5432/ragapp"
    
    # Check required env vars
    missing = []
    if not settings.dxr_api_key:
        missing.append("DXR_API_KEY")
    if not settings.dxr_datasource_id:
        missing.append("DXR_DATASOURCE_ID")
    if not settings.openai_api_key:
        missing.append("OPENAI_API_KEY")

    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        print("Please set these variables in your .env file or environment")
        return False

    print("📥 Starting document ingestion from DXR...")
    print("🔐 Using postgres superuser (bypasses RLS for data loading)")
    
    # Create ingestion database manager (uses postgres superuser)
    db_manager = IngestionDatabaseManager(settings)
    
    # Verify we're using superuser
    with db_manager.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT current_user, usesuper FROM pg_user WHERE usename = current_user')
            result = cur.fetchone()
            if result:
                user, is_super = result
                print(f"🔐 Connected as: {user} (superuser: {is_super})")
                if not is_super:
                    print("❌ Not connected as superuser! Cannot proceed.")
                    print("   Ingestion requires superuser privileges to bypass RLS")
                    return False
            else:
                print("❌ Could not verify database user! Cannot proceed.")
                return False
    
    # Create processor
    processor = DocumentProcessor(settings)
    
    print("🗑️  Clearing existing documents...")
    processor.clear_documents()
    
    # Process documents (superuser bypasses RLS automatically)
    result = processor.process_documents()
    
    if result.errors:
        print(f"⚠️  Completed with {len(result.errors)} errors:")
        for error in result.errors[:5]:  # Show first 5 errors
            print(f"   • {error}")
        if len(result.errors) > 5:
            print(f"   ... and {len(result.errors) - 5} more errors")
    
    print("✅ Ingestion completed!")
    print(f"📚 Documents processed: {result.documents_processed}")
    print(f"🧩 Chunks created: {result.chunks_created}")
    print("🎉 Ready for RAG queries!")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

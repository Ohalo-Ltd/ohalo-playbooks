"""
Document processor for data ingestion from DXR
Handles fetching documents from DXR API and processing them for storage
"""

import json
from typing import List, Dict, Any
from dataclasses import dataclass, field
from core.logging import get_logger
from core.config import yaml_config

import openai

logger = get_logger(__name__)
from dxrpy import DXRClient
from dxrpy.index import JsonSearchQuery, Hit
from psycopg2.extras import execute_values

from .config import IngestionSettings
from .database import IngestionDatabaseManager
from core.utils import split_text_by_tokens


@dataclass
class ProcessingResult:
    """Result of document processing"""
    documents_processed: int = 0
    chunks_created: int = 0
    errors: List[str] = field(default_factory=list)


class DocumentProcessor:
    """Document processor for RAG demo - uses superuser for ingestion"""

    def __init__(self, settings: IngestionSettings | None = None):
        self.settings = settings or IngestionSettings()
        self.openai_client = openai.OpenAI(api_key=self.settings.openai_api_key)
        self.db_manager = IngestionDatabaseManager(self.settings)

    def _parse_entitled_emails(self, entitlements_json) -> list[str]:
        """Parse entitled emails from DXR entitlements metadata"""
        emails = []
        if not entitlements_json:
            return emails
        try:
            if isinstance(entitlements_json, list):
                entitlements_list = entitlements_json
            else:
                entitlements_list = entitlements_json
            for entry in entitlements_list:
                if isinstance(entry, str):
                    try:
                        entitlement = json.loads(entry)
                        if entitlement.get("email"):
                            emails.append(entitlement["email"])
                    except Exception:
                        pass
                elif isinstance(entry, dict) and entry.get("email"):
                    emails.append(entry["email"])
        except Exception:
            pass
        return emails

    def _parse_extracted_employee_id(self, hit) -> str | None:
        """Parse extracted employee_id from DXR metadata using extractor id (legacy support)"""
        # Default to extractor ID "1" for backward compatibility
        extractor_id = "1"
        extracted_key = f"extracted_metadata#{extractor_id}"
        extracted_json = hit.metadata.get(extracted_key)
        if extracted_json:
            try:
                extracted = json.loads(extracted_json)
                return extracted.get("employee_id")
            except Exception:
                pass
        return None

    def _extract_custom_metadata(self, hit) -> Dict[str, Any]:
        """Extract custom metadata based on configuration"""
        metadata_configs = yaml_config.get_extracted_metadata_config()
        extracted_values = {}
        for extractor_id, config in metadata_configs.items():
            extracted_key = f"extracted_metadata#{extractor_id}"
            extracted_json = hit.metadata.get(extracted_key)
            if extracted_json:
                try:
                    if config.column_type.upper() in ["JSON", "JSONB"]:
                        extracted_values[config.column_name] = extracted_json
                    else:
                        extracted = json.loads(extracted_json)
                        if isinstance(extracted, dict):
                            if config.column_name == "extracted_employee_id":
                                value = extracted.get("employee_id")
                            else:
                                value = extracted.get("value", extracted)
                        else:
                            value = extracted
                        if value is not None:
                            extracted_values[config.column_name] = value
                except Exception as e:
                    logger.warning(f"⚠️  Failed to parse extracted metadata for {extractor_id}: {e}")
        return extracted_values

    def _get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Get embeddings for a list of texts"""
        response = self.openai_client.embeddings.create(
            model="text-embedding-3-small", input=texts
        )
        return [data.embedding for data in response.data]

    def clear_documents(self):
        """Clear all existing documents and chunks"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM document_chunks")
                cursor.execute("DELETE FROM documents")
                conn.commit()
                logger.info("🗑️  Cleared existing documents and chunks")

    def process_documents(self) -> ProcessingResult:
        """Process documents from DXR and store in database"""
        result = ProcessingResult()

        try:
            # Initialize DXR client
            client = DXRClient(
                api_key=self.settings.dxr_api_key,
                api_url=self.settings.dxr_api_url  # Use api_url instead of base_url
            )

            logger.info(f"🔍 Searching DXR datasource {self.settings.dxr_datasource_id}...")

            # Search for all documents in the datasource
            query = JsonSearchQuery(
                datasource_ids=[self.settings.dxr_datasource_id],  # Keep as string
                page_size=100
            )

            response = client.index.search(query)

            if not response.hits:
                logger.warning("⚠️  No documents found in DXR datasource")
                return result

            logger.info(f"📚 Found {len(response.hits)} documents")

            # Process documents in batches
            with self.db_manager.get_connection() as conn:
                for hit in response.hits:
                    try:
                        self._process_single_document(conn, hit, result)
                    except Exception as e:
                        error_msg = f"Failed to process document {hit.metadata.get('title', 'unknown')}: {e}"
                        result.errors.append(error_msg)
                        logger.error(f"❌ {error_msg}")
                        continue

                conn.commit()

            return result

        except Exception as e:
            error_msg = f"Document processing failed: {e}"
            result.errors.append(error_msg)
            print(f"❌ {error_msg}")
            return result

    def _process_single_document(self, conn, hit: Hit, result: ProcessingResult):
        """Process a single document from DXR"""
        title = hit.file_name or hit.metadata.get("title", "Untitled Document")

        # Get content from the correct DXR metadata field
        content = hit.metadata.get("dxr#raw_text")

        if not content or not str(content).strip():
            logger.warning(f"⚠️  Skipping document with no content: {title}")
            return

        # Ensure content is a string
        content = str(content)

        # Extract metadata
        category = hit.category
        entitled_emails = self._parse_entitled_emails(
            hit.metadata.get("computed.metadata#WHO_CAN_ACCESS")
        )
        
        # Extract custom metadata based on configuration
        custom_metadata = self._extract_custom_metadata(hit)
        
        # Legacy support: if extracted_employee_id is not in custom metadata, use old method
        if "extracted_employee_id" not in custom_metadata:
            extracted_employee_id = self._parse_extracted_employee_id(hit)
            if extracted_employee_id:
                custom_metadata["extracted_employee_id"] = extracted_employee_id

        # Build dynamic INSERT statement
        base_columns = ["title", "content", "category", "entitled_emails"]
        base_values = [title, content, category, entitled_emails]
        
        # Add custom metadata columns and values
        custom_columns = []
        custom_values = []
        for column_name, value in custom_metadata.items():
            custom_columns.append(column_name)
            custom_values.append(value)
        
        all_columns = base_columns + custom_columns
        all_values = base_values + custom_values
        
        # Create placeholders for the query
        placeholders = ", ".join(["%s"] * len(all_columns))
        columns_str = ", ".join(all_columns)
        
        # Insert document with dynamic columns
        with conn.cursor() as cursor:
            insert_sql = f"""
                INSERT INTO documents ({columns_str})
                VALUES ({placeholders})
                RETURNING id
            """
            
            cursor.execute(insert_sql, all_values)

            db_result = cursor.fetchone()
            if not db_result:
                raise Exception("Failed to insert document - no ID returned")

            document_id = db_result[0]
            result.documents_processed += 1

            logger.info(f"📄 Processing: {title} (Category: {category})")
            if custom_metadata:
                logger.info(f"  📋 Custom metadata: {list(custom_metadata.keys())}")

            # Split content into chunks
            chunks = split_text_by_tokens(content, max_tokens=500, overlap_tokens=50)

            # Get embeddings for all chunks
            embeddings = self._get_embeddings(chunks)

            # Prepare chunk data for batch insert
            chunk_data = [
                (document_id, chunk, embedding, idx)
                for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings))
            ]

            # Batch insert chunks
            execute_values(
                cursor,
                """
                INSERT INTO document_chunks (document_id, content, embedding, chunk_index)
                VALUES %s
                """,
                chunk_data,
                template=None,
                page_size=100
            )

            result.chunks_created += len(chunks)
            logger.info(f"  ✅ Created {len(chunks)} chunks")

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        with self.db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM documents")
                db_result = cursor.fetchone()
                doc_count = db_result[0] if db_result else 0

                cursor.execute("SELECT COUNT(*) FROM document_chunks") 
                db_result = cursor.fetchone()
                chunk_count = db_result[0] if db_result else 0

                cursor.execute("""
                    SELECT category, COUNT(*) 
                    FROM documents 
                    GROUP BY category 
                    ORDER BY COUNT(*) DESC
                """)
                categories = dict(cursor.fetchall())

                return {
                    "total_documents": doc_count,
                    "total_chunks": chunk_count,
                    "categories": categories
                }

"""Ingestion service for orchestrating the data pipeline."""

import logging
from typing import Any, Optional
from uuid import uuid4

from database.neo4j_client import Neo4jClient
from database.postgres_client import PostgresClient
from dxrpy.index import Index
from dxrpy.index.json_search_query import JsonSearchQuery
from dxrpy.index.search_results import Hit
from ingestion.chunker import chunk_text
from ingestion.embedder import EmbeddingService
from ingestion.graph_writer import GraphWriter
from ingestion.metadata_parser import DXRMetadataParser

logger = logging.getLogger(__name__)


def extract_entitlement_metadata(hit: Hit) -> dict[str, Any]:
    """Extract entitlement metadata from DXR hit.

    Args:
        hit: DXR Hit object from search results

    Returns:
        Dictionary with owner_email and accessible_by_emails
    """
    entitlements = {}

    # Try to get from hit._source first (standard DXR location)
    source = getattr(hit, "_source", None) or {}

    # Extract OWNER (single email)
    owner = source.get("OWNER") or hit.metadata.get("OWNER")
    if owner:
        entitlements["owner_email"] = str(owner)

    # Extract WHO_CAN_ACCESS (list of emails or comma-separated string)
    who_can_access = source.get("WHO_CAN_ACCESS") or hit.metadata.get("WHO_CAN_ACCESS")
    if who_can_access:
        if isinstance(who_can_access, list):
            # Already a list
            entitlements["accessible_by_emails"] = [
                str(email) for email in who_can_access if email
            ]
        elif isinstance(who_can_access, str):
            # Comma-separated string
            emails = [
                email.strip() for email in who_can_access.split(",") if email.strip()
            ]
            if emails:
                entitlements["accessible_by_emails"] = emails

    return entitlements


class IngestionService:
    """Service for ingesting documents into the knowledge graph."""

    def __init__(
        self,
        dxr_index: Index,
        neo4j_client: Neo4jClient,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
        metadata_parser: DXRMetadataParser,
    ):
        """Initialize ingestion service.

        Args:
            dxr_index: DXR Index for searching documents
            neo4j_client: Neo4j client
            postgres_client: PostgreSQL client
            embedding_service: Embedding service
            metadata_parser: Metadata parser
        """
        self.dxr_index = dxr_index
        self.neo4j_client = neo4j_client
        self.postgres_client = postgres_client
        self.embedding_service = embedding_service
        self.metadata_parser = metadata_parser
        self.graph_writer = GraphWriter(neo4j_client)

    async def ingest_document(
        self,
        hit: Hit,
        project_id: str,
        fetch_content: bool = True,
        extractor_id: Optional[str] = None,
        datasource_id: Optional[int] = None,
    ) -> None:
        """Ingest a single document.

        Args:
            hit: DXR Hit object from search results
            project_id: Project ID
            fetch_content: Whether to fetch and chunk document content
            extractor_id: Optional extractor ID to use for metadata parsing
            datasource_id: Optional datasource ID (integer) for content fetching
        """
        try:
            # Extract file properties from hit metadata
            file_name = hit.file_name or hit.metadata.get("ds#file_name", "unknown")
            file_id = hit.id or "unknown"  # Ensure file_id is not None

            logger.info(f"Ingesting document: {file_name} (ID: {file_id})")

            # Extract entitlement metadata
            entitlements = extract_entitlement_metadata(hit)

            # Build document properties
            doc_properties = {
                "path": hit.metadata.get("ds#object_id", ""),
                "size": hit.metadata.get("ds#size", 0),
                "mime_type": hit.metadata.get(
                    "ds#mime_type", "application/octet-stream"
                ),
                "content_sha256": hit.metadata.get("metadata#binary_hash"),
            }

            # Add entitlement properties if available
            if entitlements:
                doc_properties.update(entitlements)
                logger.info(f"Document entitlements: {entitlements}")

            # Create document node
            await self.graph_writer.create_document_node(
                document_id=file_id,
                name=file_name,
                properties=doc_properties,
                project_id=project_id,
            )

            # Parse and ingest entities from metadata
            extracted_metadata = hit.metadata.get("extractedMetadata", [])
            if extracted_metadata:
                parsed = self.metadata_parser.parse_from_dxr_file(
                    extracted_metadata,
                    extractor_id=extractor_id,
                )

                if parsed:
                    logger.info(
                        f"Found {len(parsed.entities)} entities and "
                        f"{len(parsed.relationships)} relationships"
                    )

                    # Merge entities
                    for entity in parsed.entities:
                        await self.graph_writer.merge_entity(entity)
                        await self.graph_writer.link_entity_to_document(
                            entity.id, file_id
                        )

                    # Create relationships
                    await self.graph_writer.batch_create_relationships(
                        parsed.relationships
                    )

            # Fetch and chunk content if requested
            if fetch_content:
                # Get raw_text from the hit metadata (already fetched during search)
                content = hit.metadata.get("dxr#raw_text", "")

                logger.info(
                    f"Content length for {file_name}: {len(content) if content else 0} characters"
                )

                if content:
                    # Chunk the content
                    chunks = chunk_text(content, chunk_size=1000, chunk_overlap=200)

                    logger.info(f"Created {len(chunks)} chunks")

                    # Generate embeddings in batch
                    embeddings = await self.embedding_service.generate_embeddings_batch(
                        chunks
                    )

                    # Create chunk nodes
                    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                        await self.graph_writer.create_chunk_node(
                            chunk_id=None,
                            document_id=file_id,
                            text=chunk,
                            embedding=embedding,
                            chunk_index=idx,
                        )
                else:
                    logger.warning(
                        f"No raw_text content found for {file_name}. Available metadata keys: {list(hit.metadata.keys())}"
                    )

            logger.info(f"Successfully ingested document: {file_name}")

        except Exception as e:
            logger.error(f"Failed to ingest document {file_id}: {e}")
            raise

    async def ingest_datasource(
        self,
        project_id: str,
        datasource_id: int,
        max_documents: Optional[int] = None,
        fetch_content: bool = True,
        extractor_id: Optional[str] = None,
    ) -> dict[str, int]:
        """Ingest documents from a DXR datasource.

        Args:
            project_id: Project ID
            datasource_id: DXR datasource ID (integer)
            max_documents: Maximum documents to ingest (None for all)
            fetch_content: Whether to fetch and chunk document content
            extractor_id: Optional extractor ID for metadata parsing

        Returns:
            Statistics about the ingestion
        """
        # Create ingestion job record
        job_id = str(uuid4())

        await self.postgres_client.execute(
            """
            INSERT INTO ingestion_jobs (id, project_id, status, started_at)
            VALUES ($1, $2, 'running', NOW())
            """,
            job_id,
            project_id,
        )

        try:
            # Fetch documents from DXR using indexed-files search
            logger.info(
                f"[Job {job_id}] Starting ingestion for datasource: {datasource_id}"
            )
            logger.info(
                f"[Job {job_id}] Project: {project_id}, Max documents: {max_documents}, Fetch content: {fetch_content}"
            )

            # Build search query with max page size (10k limit)
            page_size = min(max_documents or 10000, 10000)
            search_query = JsonSearchQuery(
                datasource_ids=[str(datasource_id)],
                page_number=0,
                page_size=page_size,
                query_items=[],
                refresh_index=False,
            )

            logger.info(f"[Job {job_id}] Searching DXR with page_size={page_size}...")
            try:
                result = self.dxr_index.search(search_query)
                documents = result.hits
            except Exception as search_error:
                logger.error(
                    f"[Job {job_id}] DXR search failed: {search_error}", exc_info=True
                )
                raise Exception(f"Failed to search DXR datasource: {search_error}")

            logger.info(
                f"[Job {job_id}] Found {len(documents)} documents (total available: {result.total_hits})"
            )

            if len(documents) == 0:
                logger.warning(
                    f"[Job {job_id}] No documents found in datasource {datasource_id}"
                )

            # Update job with total count
            await self.postgres_client.execute(
                """
                UPDATE ingestion_jobs
                SET documents_total = $1
                WHERE id = $2
                """,
                len(documents),
                job_id,
            )

            # Ingest each document
            processed = 0
            failed = 0

            logger.info(
                f"[Job {job_id}] Starting to process {len(documents)} documents..."
            )
            for idx, doc in enumerate(documents, 1):
                try:
                    doc_id = doc.id or "unknown"
                    doc_name = doc.file_name or doc.metadata.get(
                        "ds#file_name", "unknown"
                    )
                    logger.info(
                        f"[Job {job_id}] Processing document {idx}/{len(documents)}: {doc_name} (ID: {doc_id})"
                    )

                    await self.ingest_document(
                        doc, project_id, fetch_content, extractor_id, datasource_id
                    )
                    processed += 1
                    logger.info(
                        f"[Job {job_id}] Successfully processed {idx}/{len(documents)} - {doc_name}"
                    )

                    # Update progress
                    await self.postgres_client.execute(
                        """
                        UPDATE ingestion_jobs
                        SET documents_processed = $1, updated_at = NOW()
                        WHERE id = $2
                        """,
                        processed,
                        job_id,
                    )

                except Exception as e:
                    failed += 1
                    doc_id = doc.id or "unknown"
                    logger.error(
                        f"[Job {job_id}] Failed to ingest document {idx}/{len(documents)} (ID: {doc_id}): {e}",
                        exc_info=True,
                    )
                    continue

            # Mark job as completed
            logger.info(f"[Job {job_id}] Marking job as completed...")
            await self.postgres_client.execute(
                """
                UPDATE ingestion_jobs
                SET status = 'completed',
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
                """,
                job_id,
            )

            stats = {
                "job_id": job_id,
                "total": len(documents),
                "processed": processed,
                "failed": failed,
            }

            logger.info(f"[Job {job_id}] ✅ Ingestion completed successfully: {stats}")

            return stats

        except Exception as e:
            logger.error(
                f"[Job {job_id}] ❌ Ingestion failed with error: {e}", exc_info=True
            )
            # Mark job as failed
            try:
                await self.postgres_client.execute(
                    """
                    UPDATE ingestion_jobs
                    SET status = 'failed',
                        error_message = $1,
                        completed_at = NOW(),
                        updated_at = NOW()
                    WHERE id = $2
                    """,
                    str(e),
                    job_id,
                )
            except Exception as db_error:
                logger.error(f"[Job {job_id}] Failed to update job status: {db_error}")

            raise

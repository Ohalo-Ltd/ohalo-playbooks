"""Ingestion service for orchestrating the data pipeline."""

import json
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
    
    DXR stores entitlements as JSON-encoded strings:
    - OWNER: JSON string with {id, uuid, name, email, accountType, ...}
    - WHO_CAN_ACCESS: Array of JSON strings with the same structure

    Args:
        hit: DXR Hit object from search results

    Returns:
        Dictionary with owner_email, owner_uuid, accessible_by_group_uuids, accessible_by_user_emails
    """
    entitlements = {}

    # Try to get from hit.metadata with computed.metadata# prefix
    owner_raw = hit.metadata.get("computed.metadata#OWNER")
    who_can_access_raw = hit.metadata.get("computed.metadata#WHO_CAN_ACCESS")

    # Also try without the prefix as fallback
    if not owner_raw:
        owner_raw = hit.metadata.get("OWNER")
    if not who_can_access_raw:
        who_can_access_raw = hit.metadata.get("WHO_CAN_ACCESS")

    # Parse OWNER (JSON-encoded string)
    if owner_raw:
        try:
            if isinstance(owner_raw, str):
                owner_data = json.loads(owner_raw)
            else:
                owner_data = owner_raw

            # Extract email and uuid from owner
            if isinstance(owner_data, dict):
                if owner_data.get("email"):
                    entitlements["owner_email"] = owner_data["email"]
                if owner_data.get("uuid"):
                    entitlements["owner_uuid"] = owner_data["uuid"]

                logger.debug(f"Parsed OWNER: email={owner_data.get('email')}, uuid={owner_data.get('uuid')}")
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse OWNER metadata: {e}")

    # Parse WHO_CAN_ACCESS (array of JSON-encoded strings)
    if who_can_access_raw:
        try:
            if isinstance(who_can_access_raw, str):
                who_can_access_list = json.loads(who_can_access_raw)
            else:
                who_can_access_list = who_can_access_raw

            group_uuids = []
            user_emails = []

            if isinstance(who_can_access_list, list):
                for item in who_can_access_list:
                    # Each item is a JSON-encoded string
                    if isinstance(item, str):
                        try:
                            access_data = json.loads(item)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse WHO_CAN_ACCESS item: {item}")
                            continue
                    else:
                        access_data = item

                    if isinstance(access_data, dict):
                        account_type = access_data.get("accountType", "")
                        uuid = access_data.get("uuid", "")
                        email = access_data.get("email", "")

                        # Groups are identified by accountType="GROUP"
                        if account_type == "GROUP" and uuid:
                            group_uuids.append(uuid)
                        # Users have accountType="USER" and should have an email
                        elif account_type == "USER" and email:
                            user_emails.append(email)

                if group_uuids:
                    entitlements["accessible_by_group_uuids"] = group_uuids
                if user_emails:
                    entitlements["accessible_by_user_emails"] = user_emails

                logger.debug(f"Parsed WHO_CAN_ACCESS: {len(group_uuids)} groups, {len(user_emails)} users")

        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse WHO_CAN_ACCESS metadata: {e}")

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

            logger.info(f"Processing: {file_name}")

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
                logger.debug(
                    f"  └─ Entitlements: owner={entitlements.get('owner_email', 'N/A')}, groups={len(entitlements.get('accessible_by_group_uuids', []))}"
                )

            # Check if document already has chunks (embeddings)
            has_chunks = await self.neo4j_client.document_has_chunks(file_id)

            logger.info(
                f"  └─ Has chunks: {has_chunks}, Fetch content: {fetch_content}"
            )

            if has_chunks:
                logger.info(f"  └─ Skipping embeddings (already exists)")

            # Create/update document node (always update properties even if chunks exist)
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
                    logger.debug(
                        f"  └─ Extracted {len(parsed.entities)} entities, {len(parsed.relationships)} relationships"
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

            # Fetch and chunk content if requested and chunks don't already exist
            if fetch_content and not has_chunks:
                # Get raw_text from the hit metadata (already fetched during search)
                content = hit.metadata.get("dxr#raw_text", "")

                logger.info(
                    f"  └─ Content available: {len(content) > 0}, Length: {len(content)} chars"
                )

                if content:
                    # Chunk the content
                    chunks = chunk_text(content, chunk_size=1000, chunk_overlap=200)
                    logger.info(f"  └─ Creating {len(chunks)} chunks with embeddings")

                    # Generate embeddings in batch
                    logger.debug(
                        f"  └─ Generating embeddings for {len(chunks)} chunks..."
                    )
                    embeddings = await self.embedding_service.generate_embeddings_batch(
                        chunks
                    )
                    logger.debug(f"  └─ Embeddings generated: {len(embeddings)}")

                    # Create chunk nodes
                    logger.debug(f"  └─ Creating chunk nodes in Neo4j...")
                    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
                        await self.graph_writer.create_chunk_node(
                            chunk_id=None,
                            document_id=file_id,
                            text=chunk,
                            embedding=embedding,
                            chunk_index=idx,
                        )
                    logger.info(f"  └─ ✓ Created {len(chunks)} chunks")
                else:
                    logger.warning(f"  └─ No content available for chunking")
            elif fetch_content and has_chunks:
                logger.debug(f"  └─ Skipping content fetch (chunks exist)")
            elif not fetch_content:
                logger.debug(f"  └─ Content fetch disabled")

        except Exception as e:
            logger.error(f"Failed to ingest {file_name}: {e}")
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
                    doc_name = doc.file_name or doc.metadata.get(
                        "ds#file_name", "unknown"
                    )
                    logger.info(f"[Job {job_id}] [{idx}/{len(documents)}] {doc_name}")

                    await self.ingest_document(
                        doc, project_id, fetch_content, extractor_id, datasource_id
                    )
                    processed += 1

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

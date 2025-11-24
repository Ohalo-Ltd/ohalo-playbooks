"""Ingestion service for orchestrating the data pipeline."""

import logging
from typing import Optional
from uuid import uuid4

from database.neo4j_client import Neo4jClient
from database.postgres_client import PostgresClient
from ingestion.chunker import chunk_text
from ingestion.dxr_client import DXRClient, DXRFile
from ingestion.embedder import EmbeddingService
from ingestion.graph_writer import GraphWriter
from ingestion.metadata_parser import DXRMetadataParser

logger = logging.getLogger(__name__)


class IngestionService:
    """Service for ingesting documents into the knowledge graph."""

    def __init__(
        self,
        dxr_client: DXRClient,
        neo4j_client: Neo4jClient,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
        metadata_parser: DXRMetadataParser,
    ):
        """Initialize ingestion service.

        Args:
            dxr_client: DXR client
            neo4j_client: Neo4j client
            postgres_client: PostgreSQL client
            embedding_service: Embedding service
            metadata_parser: Metadata parser
        """
        self.dxr_client = dxr_client
        self.neo4j_client = neo4j_client
        self.postgres_client = postgres_client
        self.embedding_service = embedding_service
        self.metadata_parser = metadata_parser
        self.graph_writer = GraphWriter(neo4j_client)

    async def ingest_document(
        self,
        dxr_file: DXRFile,
        project_id: str,
        fetch_content: bool = True,
    ) -> None:
        """Ingest a single document.

        Args:
            dxr_file: DXR file to ingest
            project_id: Project ID
            fetch_content: Whether to fetch and chunk document content
        """
        try:
            logger.info(f"Ingesting document: {dxr_file.name} (ID: {dxr_file.id})")

            # Create document node
            await self.graph_writer.create_document_node(
                document_id=dxr_file.id,
                name=dxr_file.name,
                properties={
                    "path": dxr_file.path,
                    "size": dxr_file.size,
                    "mime_type": dxr_file.mime_type,
                    "categories": dxr_file.categories,
                },
            )

            # Parse and ingest entities from metadata
            if dxr_file.extracted_metadata:
                parsed = self.metadata_parser.parse_from_dxr_file(
                    dxr_file.extracted_metadata
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
                            entity.id, dxr_file.id
                        )

                    # Create relationships
                    await self.graph_writer.batch_create_relationships(
                        parsed.relationships
                    )

            # Fetch and chunk content if requested
            if fetch_content:
                content = await self.dxr_client.fetch_document_content(dxr_file.id)

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
                            document_id=dxr_file.id,
                            text=chunk,
                            embedding=embedding,
                            chunk_index=idx,
                        )

            logger.info(f"Successfully ingested document: {dxr_file.name}")

        except Exception as e:
            logger.error(f"Failed to ingest document {dxr_file.id}: {e}")
            raise

    async def ingest_datasource(
        self,
        project_id: str,
        datasource_id: str,
        max_documents: Optional[int] = None,
        fetch_content: bool = True,
    ) -> dict[str, int]:
        """Ingest all documents from a DXR datasource.

        Args:
            project_id: Project ID
            datasource_id: DXR datasource ID
            max_documents: Maximum documents to ingest (None for all)
            fetch_content: Whether to fetch and chunk document content

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
            # Fetch documents from DXR
            logger.info(f"Fetching documents from datasource: {datasource_id}")

            documents = await self.dxr_client.fetch_all_documents(
                datasource_id=datasource_id,
                max_documents=max_documents,
            )

            logger.info(f"Found {len(documents)} documents")

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

            for doc in documents:
                try:
                    await self.ingest_document(doc, project_id, fetch_content)
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
                    logger.error(f"Failed to ingest document {doc.id}: {e}")
                    failed += 1
                    continue

            # Mark job as completed
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

            logger.info(f"Ingestion completed: {stats}")

            return stats

        except Exception as e:
            # Mark job as failed
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

            raise

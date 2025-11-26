"""API endpoints for ingestion functionality."""

import logging
from typing import Optional, AsyncGenerator

logger = logging.getLogger(__name__)

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel

from database.neo4j_client import Neo4jClient
from database.postgres_client import PostgresClient
from dxrpy.dxr_client import DXRHttpClient
from dxrpy.index import Index
from ingestion.embedder import EmbeddingService
from ingestion.metadata_parser import DXRMetadataParser
from ingestion.service import IngestionService

from core.config import settings

router = APIRouter(prefix="/api/ingestion", tags=["ingestion"])


class IngestionRequest(BaseModel):
    """Ingestion request model."""

    project_id: str
    datasource_id: int  # Integer ID for indexed-files/search endpoint
    extractor_id: str = "default"
    max_documents: Optional[int] = None
    fetch_content: bool = True


class IngestionResponse(BaseModel):
    """Ingestion response model."""

    job_id: str
    message: str


class IngestionStatus(BaseModel):
    """Ingestion status model."""

    job_id: str
    status: str
    documents_processed: int
    documents_total: int


async def get_ingestion_service() -> AsyncGenerator[IngestionService, None]:
    """Get ingestion service dependency."""
    # Initialize DXR singleton client
    DXRHttpClient.get_instance(
        api_url=settings.dxr_base_url,
        api_key=settings.dxr_api_key,
        ignore_ssl=False,
    )
    dxr_index = Index()

    neo4j_client = Neo4jClient()
    postgres_client = PostgresClient()
    embedding_service = EmbeddingService()
    metadata_parser = DXRMetadataParser()

    await neo4j_client.connect()
    await postgres_client.connect()

    try:
        yield IngestionService(
            dxr_index=dxr_index,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )
    finally:
        await neo4j_client.close()
        await postgres_client.close()


@router.post("/start", response_model=IngestionResponse)
async def start_ingestion(
    request: IngestionRequest,
    background_tasks: BackgroundTasks,
    service: IngestionService = Depends(get_ingestion_service),
) -> IngestionResponse:
    """Start document ingestion from DXR datasource.

    Args:
        request: Ingestion request
        background_tasks: Background task manager
        service: Ingestion service

    Returns:
        Ingestion response with job ID
    """
    try:
        # Fetch project to get DXR credentials
        from uuid import UUID
        from core.crypto import get_encryption_key

        pg_client = PostgresClient()
        await pg_client.connect()

        try:
            encryption_key = get_encryption_key()
            project = await pg_client.fetchrow(
                """
                SELECT id, dxr_url,
                       CASE 
                           WHEN dxr_api_token IS NOT NULL 
                           THEN pgp_sym_decrypt(dxr_api_token, $2)::text 
                           ELSE NULL 
                       END as dxr_api_token,
                       dxr_datasource_id, dxr_extractor_id
                FROM projects
                WHERE id = $1
                """,
                UUID(request.project_id),
                encryption_key,
            )

            if not project:
                raise HTTPException(status_code=404, detail="Project not found")

            if not project["dxr_api_token"]:
                raise HTTPException(
                    status_code=400, detail="Project DXR API token not configured"
                )

            # Initialize DXR client with project-specific credentials
            DXRHttpClient.get_instance(
                api_url=(
                    project["dxr_url"] if project["dxr_url"] else settings.dxr_base_url
                ),
                api_key=project["dxr_api_token"],
                ignore_ssl=False,
            )
            # Create new Index instance with the initialized client
            service.dxr_index = Index()

            # Use project's datasource_id if not provided in request
            datasource_id = request.datasource_id or project["dxr_datasource_id"]

            if not datasource_id:
                raise HTTPException(
                    status_code=400,
                    detail="Datasource ID not provided and not configured in project",
                )

        finally:
            await pg_client.close()

        # Wrapper to catch exceptions in background task
        async def run_ingestion_with_logging():
            print(
                f"🔥 BACKGROUND TASK STARTED - Project: {request.project_id}, Datasource: {datasource_id}"
            )
            try:
                logger.info(
                    f"Background ingestion started for project {request.project_id}, datasource {datasource_id}"
                )
                result = await service.ingest_datasource(
                    project_id=request.project_id,
                    datasource_id=datasource_id,
                    max_documents=request.max_documents,
                    fetch_content=request.fetch_content,
                    extractor_id=project["dxr_extractor_id"],
                )
                logger.info(f"Background ingestion completed: {result}")
                print(f"✅ BACKGROUND TASK COMPLETED - Result: {result}")
            except Exception as e:
                logger.error(f"Background ingestion failed: {e}", exc_info=True)
                print(f"❌ BACKGROUND TASK FAILED - Error: {e}")
                raise

        # Start ingestion in background
        background_tasks.add_task(run_ingestion_with_logging)

        return IngestionResponse(
            job_id="pending",
            message="Ingestion started in background",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{job_id}", response_model=IngestionStatus)
async def get_ingestion_status(
    job_id: str,
    postgres_client: PostgresClient = Depends(PostgresClient),
) -> IngestionStatus:
    """Get ingestion job status.

    Args:
        job_id: Job ID
        postgres_client: PostgreSQL client

    Returns:
        Ingestion status
    """
    await postgres_client.connect()

    try:
        job = await postgres_client.fetchrow(
            """
            SELECT status, documents_processed, documents_total
            FROM ingestion_jobs
            WHERE id = $1
            """,
            job_id,
        )

        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        return IngestionStatus(
            job_id=job_id,
            status=job["status"],
            documents_processed=job["documents_processed"] or 0,
            documents_total=job["documents_total"] or 0,
        )

    finally:
        await postgres_client.close()

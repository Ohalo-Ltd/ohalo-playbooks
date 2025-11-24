"""API endpoints for ingestion functionality."""

from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel

from database.neo4j_client import Neo4jClient
from database.postgres_client import PostgresClient
from ingestion.dxr_client import DXRClient
from ingestion.embedder import EmbeddingService
from ingestion.metadata_parser import DXRMetadataParser
from ingestion.service import IngestionService

router = APIRouter(prefix="/api/ingestion", tags=["ingestion"])


class IngestionRequest(BaseModel):
    """Ingestion request model."""

    project_id: str
    datasource_id: str
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


async def get_ingestion_service() -> IngestionService:
    """Get ingestion service dependency."""
    dxr_client = DXRClient()
    neo4j_client = Neo4jClient()
    postgres_client = PostgresClient()
    embedding_service = EmbeddingService()
    metadata_parser = DXRMetadataParser()

    await neo4j_client.connect()
    await postgres_client.connect()

    try:
        yield IngestionService(
            dxr_client=dxr_client,
            neo4j_client=neo4j_client,
            postgres_client=postgres_client,
            embedding_service=embedding_service,
            metadata_parser=metadata_parser,
        )
    finally:
        await dxr_client.close()
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
        # Start ingestion in background
        background_tasks.add_task(
            service.ingest_datasource,
            project_id=request.project_id,
            datasource_id=request.datasource_id,
            max_documents=request.max_documents,
            fetch_content=request.fetch_content,
        )

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

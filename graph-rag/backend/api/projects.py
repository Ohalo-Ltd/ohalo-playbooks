"""API endpoints for project management."""

import logging
from datetime import datetime
from typing import Optional
from typing import AsyncGenerator
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.crypto import get_encryption_key
from database.postgres_client import PostgresClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/projects", tags=["projects"])


class ProjectCreate(BaseModel):
    """Project creation request."""

    name: str
    description: Optional[str] = None
    system_prompt: Optional[str] = None
    dxr_url: Optional[str] = None
    dxr_api_token: Optional[str] = None
    dxr_datasource_id: Optional[str] = None
    dxr_extractor_id: Optional[str] = None
    entitlements_enabled: Optional[bool] = None


class ProjectUpdate(BaseModel):
    """Project update request."""

    name: Optional[str] = None
    description: Optional[str] = None
    system_prompt: Optional[str] = None
    dxr_url: Optional[str] = None
    dxr_api_token: Optional[str] = None
    dxr_datasource_id: Optional[str] = None
    dxr_extractor_id: Optional[str] = None
    entitlements_enabled: Optional[bool] = None


class ProjectResponse(BaseModel):
    """Project response model."""

    id: str
    name: str
    description: Optional[str] = None
    system_prompt: Optional[str] = None
    dxr_url: Optional[str] = None
    dxr_api_token: Optional[str] = None
    dxr_datasource_id: Optional[str] = None
    dxr_extractor_id: Optional[str] = None
    entitlements_enabled: bool = False
    created_at: datetime
    updated_at: datetime


async def get_postgres_client() -> AsyncGenerator[PostgresClient, None]:
    """Get Postgres client dependency."""
    client = PostgresClient()
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


@router.get("", response_model=list[ProjectResponse])
async def list_projects(
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> list[ProjectResponse]:
    """List all projects.

    Args:
        pg_client: Postgres client

    Returns:
        List of projects
    """
    try:
        encryption_key = get_encryption_key()
        rows = await pg_client.fetch(
            """
            SELECT id, name, description, system_prompt, dxr_url, 
                   CASE 
                       WHEN dxr_api_token IS NOT NULL 
                       THEN pgp_sym_decrypt(dxr_api_token, $1)::text 
                       ELSE NULL 
                   END as dxr_api_token,
                   dxr_datasource_id, dxr_extractor_id, entitlements_enabled, 
                   created_at, updated_at
            FROM projects
            ORDER BY created_at DESC
            """,
            encryption_key,
        )

        return [
            ProjectResponse(
                id=str(row["id"]),
                name=row["name"],
                description=row["description"],
                system_prompt=row["system_prompt"],
                dxr_url=row["dxr_url"],
                dxr_api_token=row["dxr_api_token"],
                dxr_datasource_id=row["dxr_datasource_id"],
                dxr_extractor_id=row["dxr_extractor_id"],
                entitlements_enabled=row["entitlements_enabled"] or False,
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    except Exception as e:
        logger.error(f"Failed to list projects: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: str,
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> ProjectResponse:
    """Get a specific project.

    Args:
        project_id: Project ID
        pg_client: Postgres client

    Returns:
        Project details
    """
    try:
        encryption_key = get_encryption_key()
        row = await pg_client.fetchrow(
            """
            SELECT id, name, description, system_prompt, dxr_url,
                   CASE 
                       WHEN dxr_api_token IS NOT NULL 
                       THEN pgp_sym_decrypt(dxr_api_token, $2)::text 
                       ELSE NULL 
                   END as dxr_api_token,
                   dxr_datasource_id, dxr_extractor_id, entitlements_enabled,
                   created_at, updated_at
            FROM projects
            WHERE id = $1
            """,
            UUID(project_id),
            encryption_key,
        )

        if not row:
            raise HTTPException(status_code=404, detail="Project not found")

        return ProjectResponse(
            id=str(row["id"]),
            name=row["name"],
            description=row["description"],
            system_prompt=row["system_prompt"],
            dxr_url=row["dxr_url"],
            dxr_api_token=row["dxr_api_token"],
            dxr_datasource_id=row["dxr_datasource_id"],
            dxr_extractor_id=row["dxr_extractor_id"],
            entitlements_enabled=row["entitlements_enabled"] or False,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get project {project_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("", response_model=ProjectResponse, status_code=201)
async def create_project(
    project: ProjectCreate,
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> ProjectResponse:
    """Create a new project.

    Args:
        project: Project creation data
        pg_client: Postgres client

    Returns:
        Created project
    """
    try:
        encryption_key = get_encryption_key()
        row = await pg_client.fetchrow(
            """
            INSERT INTO projects (name, description, system_prompt, dxr_url, dxr_api_token, dxr_datasource_id, dxr_extractor_id, entitlements_enabled)
            VALUES ($1, $2, $3, $4, pgp_sym_encrypt($5, $9), $6, $7, $8)
            RETURNING id, name, description, system_prompt, dxr_url,
                      CASE 
                          WHEN dxr_api_token IS NOT NULL 
                          THEN pgp_sym_decrypt(dxr_api_token, $9)::text 
                          ELSE NULL 
                      END as dxr_api_token,
                      dxr_datasource_id, dxr_extractor_id, entitlements_enabled,
                      created_at, updated_at
            """,
            project.name,
            project.description,
            project.system_prompt,
            project.dxr_url,
            project.dxr_api_token,
            project.dxr_datasource_id,
            project.dxr_extractor_id,
            project.entitlements_enabled,
            encryption_key,
        )

        if not row:
            raise HTTPException(status_code=500, detail="Failed to create project")

        return ProjectResponse(
            id=str(row["id"]),
            name=row["name"],
            description=row["description"],
            system_prompt=row["system_prompt"],
            dxr_url=row["dxr_url"],
            dxr_api_token=row["dxr_api_token"],
            dxr_datasource_id=row["dxr_datasource_id"],
            dxr_extractor_id=row["dxr_extractor_id"],
            entitlements_enabled=row["entitlements_enabled"] or False,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: str,
    project_update: ProjectUpdate,
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> ProjectResponse:
    """Update a project.

    Args:
        project_id: Project ID
        project_update: Fields to update
        pg_client: Postgres client

    Returns:
        Updated project
    """
    try:
        encryption_key = get_encryption_key()

        # Check if project exists
        existing = await pg_client.fetchrow(
            "SELECT id FROM projects WHERE id = $1",
            UUID(project_id),
        )

        if not existing:
            raise HTTPException(status_code=404, detail="Project not found")

        # Build update query dynamically based on provided fields
        update_fields = []
        values = []
        param_count = 1
        encryption_param_index = None

        if project_update.name is not None:
            update_fields.append(f"name = ${param_count}")
            values.append(project_update.name)
            param_count += 1

        if project_update.description is not None:
            update_fields.append(f"description = ${param_count}")
            values.append(project_update.description)
            param_count += 1

        if project_update.system_prompt is not None:
            update_fields.append(f"system_prompt = ${param_count}")
            values.append(project_update.system_prompt)
            param_count += 1

        if project_update.dxr_url is not None:
            update_fields.append(f"dxr_url = ${param_count}")
            values.append(project_update.dxr_url)
            param_count += 1

        if project_update.dxr_api_token is not None:
            # Store the token value index and encryption key index
            token_param = param_count
            param_count += 1
            encryption_param_index = param_count
            param_count += 1
            update_fields.append(
                f"dxr_api_token = pgp_sym_encrypt(${token_param}, ${encryption_param_index})"
            )
            values.append(project_update.dxr_api_token)
            values.append(encryption_key)

        if project_update.dxr_datasource_id is not None:
            update_fields.append(f"dxr_datasource_id = ${param_count}")
            values.append(project_update.dxr_datasource_id)
            param_count += 1

        if project_update.dxr_extractor_id is not None:
            update_fields.append(f"dxr_extractor_id = ${param_count}")
            values.append(project_update.dxr_extractor_id)
            param_count += 1

        if project_update.entitlements_enabled is not None:
            update_fields.append(f"entitlements_enabled = ${param_count}")
            values.append(project_update.entitlements_enabled)
            param_count += 1

        if not update_fields:
            # No fields to update, just return current state
            row = await pg_client.fetchrow(
                """
                SELECT id, name, description, system_prompt, dxr_url,
                       CASE 
                           WHEN dxr_api_token IS NOT NULL 
                           THEN pgp_sym_decrypt(dxr_api_token, $2)::text 
                           ELSE NULL 
                       END as dxr_api_token,
                       dxr_datasource_id, dxr_extractor_id, entitlements_enabled,
                       created_at, updated_at
                FROM projects
                WHERE id = $1
                """,
                UUID(project_id),
                encryption_key,
            )
        else:
            # Add updated_at
            update_fields.append(f"updated_at = ${param_count}")
            values.append(datetime.utcnow())
            param_count += 1

            # Add project_id as last parameter
            values.append(UUID(project_id))
            project_id_param = param_count
            param_count += 1

            # Add encryption key if not already added (for RETURNING clause)
            if encryption_param_index is None:
                values.append(encryption_key)
                encryption_param_index = param_count

            query = f"""
                UPDATE projects
                SET {', '.join(update_fields)}
                WHERE id = ${project_id_param}
                RETURNING id, name, description, system_prompt, dxr_url,
                          CASE 
                              WHEN dxr_api_token IS NOT NULL 
                              THEN pgp_sym_decrypt(dxr_api_token, ${encryption_param_index})::text 
                              ELSE NULL 
                          END as dxr_api_token,
                          dxr_datasource_id, dxr_extractor_id, entitlements_enabled,
                          created_at, updated_at
            """

            row = await pg_client.fetchrow(query, *values)

        if not row:
            raise HTTPException(status_code=500, detail="Failed to update project")

        return ProjectResponse(
            id=str(row["id"]),
            name=row["name"],
            description=row["description"],
            system_prompt=row["system_prompt"],
            dxr_url=row["dxr_url"],
            dxr_api_token=row["dxr_api_token"],
            dxr_datasource_id=row["dxr_datasource_id"],
            dxr_extractor_id=row["dxr_extractor_id"],
            entitlements_enabled=row["entitlements_enabled"] or False,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{project_id}", status_code=204)
async def delete_project(
    project_id: str,
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> None:
    """Delete a project.

    Args:
        project_id: Project ID
        pg_client: Postgres client
    """
    try:
        result = await pg_client.execute(
            "DELETE FROM projects WHERE id = $1",
            UUID(project_id),
        )

        # Check if any rows were deleted
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Project not found")

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

"""API endpoints for project management."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from database.postgres_client import PostgresClient

router = APIRouter(prefix="/api/projects", tags=["projects"])


class ProjectCreate(BaseModel):
    """Project creation request."""

    name: str
    description: Optional[str] = None
    system_prompt: Optional[str] = None


class ProjectUpdate(BaseModel):
    """Project update request."""

    name: Optional[str] = None
    description: Optional[str] = None
    system_prompt: Optional[str] = None


class ProjectResponse(BaseModel):
    """Project response model."""

    id: str
    name: str
    description: Optional[str] = None
    system_prompt: Optional[str] = None
    created_at: datetime
    updated_at: datetime


async def get_postgres_client() -> PostgresClient:
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
        rows = await pg_client.fetch(
            """
            SELECT id, name, description, system_prompt, created_at, updated_at
            FROM projects
            ORDER BY created_at DESC
            """
        )

        return [
            ProjectResponse(
                id=str(row["id"]),
                name=row["name"],
                description=row["description"],
                system_prompt=row["system_prompt"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        row = await pg_client.fetchrow(
            """
            SELECT id, name, description, system_prompt, created_at, updated_at
            FROM projects
            WHERE id = $1
            """,
            UUID(project_id),
        )

        if not row:
            raise HTTPException(status_code=404, detail="Project not found")

        return ProjectResponse(
            id=str(row["id"]),
            name=row["name"],
            description=row["description"],
            system_prompt=row["system_prompt"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        row = await pg_client.fetchrow(
            """
            INSERT INTO projects (name, description, system_prompt)
            VALUES ($1, $2, $3)
            RETURNING id, name, description, system_prompt, created_at, updated_at
            """,
            project.name,
            project.description,
            project.system_prompt,
        )

        return ProjectResponse(
            id=str(row["id"]),
            name=row["name"],
            description=row["description"],
            system_prompt=row["system_prompt"],
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

        if not update_fields:
            # No fields to update, just return current state
            row = await pg_client.fetchrow(
                """
                SELECT id, name, description, system_prompt, created_at, updated_at
                FROM projects
                WHERE id = $1
                """,
                UUID(project_id),
            )
        else:
            # Add updated_at
            update_fields.append(f"updated_at = ${param_count}")
            values.append(datetime.utcnow())
            param_count += 1

            # Add project_id as last parameter
            values.append(UUID(project_id))

            query = f"""
                UPDATE projects
                SET {', '.join(update_fields)}
                WHERE id = ${param_count}
                RETURNING id, name, description, system_prompt, created_at, updated_at
            """

            row = await pg_client.fetchrow(query, *values)

        return ProjectResponse(
            id=str(row["id"]),
            name=row["name"],
            description=row["description"],
            system_prompt=row["system_prompt"],
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

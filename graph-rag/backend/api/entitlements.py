"""API endpoints for entitlements management."""

import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import AsyncGenerator

from database.postgres_client import PostgresClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/projects", tags=["entitlements"])


class ProjectUser(BaseModel):
    """Project user model."""

    id: str
    project_id: str
    name: str
    email: str
    role: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class ProjectUserCreate(BaseModel):
    """Create user request."""

    name: str
    email: str
    role: Optional[str] = None


class ProjectUserUpdate(BaseModel):
    """Update user request."""

    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None


class ProjectGroup(BaseModel):
    """Project group model."""

    id: str
    project_id: str
    name: str
    code: str
    created_at: datetime
    updated_at: datetime


class ProjectGroupCreate(BaseModel):
    """Create group request."""

    name: str
    code: str


class ProjectGroupUpdate(BaseModel):
    """Update group request."""

    name: Optional[str] = None
    code: Optional[str] = None


async def get_postgres_client() -> AsyncGenerator[PostgresClient, None]:
    """Get Postgres client dependency."""
    client = PostgresClient()
    await client.connect()
    try:
        yield client
    finally:
        await client.close()


# User endpoints
@router.get("/{project_id}/entitlements/users", response_model=list[ProjectUser])
async def list_project_users(
    project_id: UUID,
    pg_client: PostgresClient = Depends(get_postgres_client),
):
    """List all users for a project.

    Args:
        project_id: Project ID
        pg_client: Postgres client

    Returns:
        List of users
    """
    try:
        rows = await pg_client.fetch(
            """
            SELECT id, project_id, name, email, role, created_at, updated_at
            FROM project_users
            WHERE project_id = $1
            ORDER BY name
            """,
            project_id,
        )

        return [
            ProjectUser(
                id=str(row["id"]),
                project_id=str(row["project_id"]),
                name=row["name"],
                email=row["email"],
                role=row["role"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except Exception as e:
        logger.error(f"Failed to list users for project {project_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/{project_id}/entitlements/users", response_model=ProjectUser, status_code=201)
async def create_project_user(
    project_id: UUID,
    user: ProjectUserCreate,
    pg_client: PostgresClient = Depends(get_postgres_client),
):
    """Create a new user for a project.

    Args:
        project_id: Project ID
        user: User creation data
        pg_client: Postgres client

    Returns:
        Created user
    """
    try:
        # Verify project exists
        project_exists = await pg_client.fetchrow(
            "SELECT id FROM projects WHERE id = $1",
            project_id,
        )

        if not project_exists:
            raise HTTPException(status_code=404, detail="Project not found")

        row = await pg_client.fetchrow(
            """
            INSERT INTO project_users (project_id, name, email, role)
            VALUES ($1, $2, $3, $4)
            RETURNING id, project_id, name, email, role, created_at, updated_at
            """,
            project_id,
            user.name,
            user.email,
            user.role,
        )

        if not row:
            raise HTTPException(status_code=500, detail="Failed to create user")

        return ProjectUser(
            id=str(row["id"]),
            project_id=str(row["project_id"]),
            name=row["name"],
            email=row["email"],
            role=row["role"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except Exception as e:
        error_str = str(e)
        if "unique_project_user_email" in error_str:
            raise HTTPException(
                status_code=400,
                detail=f"User with email {user.email} already exists in this project",
            )
        logger.error(f"Failed to create user for project {project_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {error_str}")


@router.get("/{project_id}/entitlements/users/{user_id}", response_model=ProjectUser)
async def get_project_user(
    project_id: UUID,
    user_id: UUID,
    pg_client: PostgresClient = Depends(get_postgres_client),
):
    """Get a specific user.

    Args:
        project_id: Project ID
        user_id: User ID
        pg_client: Postgres client

    Returns:
        User details
    """
    try:
        row = await pg_client.fetchrow(
            """
            SELECT id, project_id, name, email, role, created_at, updated_at
            FROM project_users
            WHERE id = $1 AND project_id = $2
            """,
            user_id,
            project_id,
        )

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return ProjectUser(
            id=str(row["id"]),
            project_id=str(row["project_id"]),
            name=row["name"],
            email=row["email"],
            role=row["role"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.patch("/{project_id}/entitlements/users/{user_id}", response_model=ProjectUser)
async def update_user(
    project_id: UUID,
    user_id: UUID,
    user: ProjectUserUpdate,
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> ProjectUser:
    """Update a user.

    Args:
        project_id: Project ID
        user_id: User ID
        user: User update data
        pg_client: Postgres client

    Returns:
        Updated user
    """
    try:
        # Build dynamic update query
        update_fields = []
        params: list = [user_id, project_id]
        param_idx = 3
        
        if user.name is not None:
            update_fields.append(f"name = ${param_idx}")
            params.append(user.name)
            param_idx += 1
        
        if user.email is not None:
            update_fields.append(f"email = ${param_idx}")
            params.append(user.email)
            param_idx += 1
        
        if user.role is not None:
            update_fields.append(f"role = ${param_idx}")
            params.append(user.role)
            param_idx += 1
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        update_fields.append("updated_at = NOW()")
        
        query = f"""
            UPDATE project_users
            SET {', '.join(update_fields)}
            WHERE id = $1 AND project_id = $2
            RETURNING id, project_id, name, email, role, created_at, updated_at
        """
        
        row = await pg_client.fetchrow(query, *params)

        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        return ProjectUser(
            id=str(row["id"]),
            project_id=str(row["project_id"]),
            name=row["name"],
            email=row["email"],
            role=row["role"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")
    except HTTPException:
        raise
    except Exception as e:
        error_str = str(e)
        if "unique_project_user_email" in error_str:
            raise HTTPException(
                status_code=400,
                detail=f"User with email {user.email} already exists in this project",
            )
        logger.error(f"Failed to update user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {error_str}")


@router.delete("/{project_id}/entitlements/users/{user_id}", status_code=204)
async def delete_project_user(
    project_id: UUID,
    user_id: UUID,
    pg_client: PostgresClient = Depends(get_postgres_client),
):
    """Delete a user.

    Args:
        project_id: Project ID
        user_id: User ID
        pg_client: Postgres client
    """
    try:
        result = await pg_client.execute(
            "DELETE FROM project_users WHERE id = $1 AND project_id = $2",
            user_id,
            project_id,
        )

        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="User not found")

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# Group endpoints
@router.get("/{project_id}/entitlements/groups", response_model=list[ProjectGroup])
async def list_project_groups(
    project_id: UUID,
    pg_client: PostgresClient = Depends(get_postgres_client),
):
    """List all groups for a project.

    Args:
        project_id: Project ID
        pg_client: Postgres client

    Returns:
        List of groups
    """
    try:
        rows = await pg_client.fetch(
            """
            SELECT id, project_id, name, code, created_at, updated_at
            FROM project_groups
            WHERE project_id = $1
            ORDER BY name
            """,
            project_id,
        )

        return [
            ProjectGroup(
                id=str(row["id"]),
                project_id=str(row["project_id"]),
                name=row["name"],
                code=row["code"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except Exception as e:
        logger.error(f"Failed to list groups for project {project_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/{project_id}/entitlements/groups", response_model=ProjectGroup, status_code=201)
async def create_project_group(
    project_id: UUID,
    group: ProjectGroupCreate,
    pg_client: PostgresClient = Depends(get_postgres_client),
):
    """Create a new group for a project.

    Args:
        project_id: Project ID
        group: Group creation data
        pg_client: Postgres client

    Returns:
        Created group
    """
    try:
        # Verify project exists
        project_exists = await pg_client.fetchrow(
            "SELECT id FROM projects WHERE id = $1",
            project_id,
        )

        if not project_exists:
            raise HTTPException(status_code=404, detail="Project not found")

        row = await pg_client.fetchrow(
            """
            INSERT INTO project_groups (project_id, name, code)
            VALUES ($1, $2, $3)
            RETURNING id, project_id, name, code, created_at, updated_at
            """,
            project_id,
            group.name,
            group.code,
        )

        if not row:
            raise HTTPException(status_code=500, detail="Failed to create group")

        return ProjectGroup(
            id=str(row["id"]),
            project_id=str(row["project_id"]),
            name=row["name"],
            code=row["code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid project ID format")
    except Exception as e:
        error_str = str(e)
        if "unique_project_group_code" in error_str:
            raise HTTPException(
                status_code=400,
                detail=f"Group with code {group.code} already exists in this project",
            )
        logger.error(f"Failed to create group for project {project_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {error_str}")


@router.get("/{project_id}/entitlements/groups/{group_id}", response_model=ProjectGroup)
async def get_project_group(
    project_id: UUID,
    group_id: UUID,
    pg_client: PostgresClient = Depends(get_postgres_client),
):
    """Get a specific group.

    Args:
        project_id: Project ID
        group_id: Group ID
        pg_client: Postgres client

    Returns:
        Group details
    """
    try:
        row = await pg_client.fetchrow(
            """
            SELECT id, project_id, name, code, created_at, updated_at
            FROM project_groups
            WHERE id = $1 AND project_id = $2
            """,
            group_id,
            project_id,
        )

        if not row:
            raise HTTPException(status_code=404, detail="Group not found")

        return ProjectGroup(
            id=str(row["id"]),
            project_id=str(row["project_id"]),
            name=row["name"],
            code=row["code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get group {group_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.patch("/{project_id}/entitlements/groups/{group_id}", response_model=ProjectGroup)
async def update_group(
    project_id: UUID,
    group_id: UUID,
    group: ProjectGroupUpdate,
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> ProjectGroup:
    """Update a group.

    Args:
        project_id: Project ID
        group_id: Group ID
        group: Group update data
        pg_client: Postgres client

    Returns:
        Updated group
    """
    try:
        # Build dynamic update query
        update_fields = []
        params: list = [group_id, project_id]
        param_idx = 3
        
        if group.name is not None:
            update_fields.append(f"name = ${param_idx}")
            params.append(group.name)
            param_idx += 1
        
        if group.code is not None:
            update_fields.append(f"code = ${param_idx}")
            params.append(group.code)
            param_idx += 1
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        update_fields.append("updated_at = NOW()")
        
        query = f"""
            UPDATE project_groups
            SET {', '.join(update_fields)}
            WHERE id = $1 AND project_id = $2
            RETURNING id, project_id, name, code, created_at, updated_at
        """
        
        row = await pg_client.fetchrow(query, *params)

        if not row:
            raise HTTPException(status_code=404, detail="Group not found")

        return ProjectGroup(
            id=str(row["id"]),
            project_id=str(row["project_id"]),
            name=row["name"],
            code=row["code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")
    except HTTPException:
        raise
    except Exception as e:
        error_str = str(e)
        if "unique_project_group_code" in error_str:
            raise HTTPException(
                status_code=400,
                detail=f"Group with code {group.code} already exists in this project",
            )
        logger.error(f"Failed to update group {group_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {error_str}")


@router.delete("/{project_id}/entitlements/groups/{group_id}", status_code=204)
async def delete_group(
    project_id: UUID,
    group_id: UUID,
    pg_client: PostgresClient = Depends(get_postgres_client),
) -> None:
    """Delete a group.

    Args:
        project_id: Project ID
        group_id: Group ID
        pg_client: Postgres client
    """
    try:
        result = await pg_client.execute(
            "DELETE FROM project_groups WHERE id = $1 AND project_id = $2",
            group_id,
            project_id,
        )

        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Group not found")

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid ID format")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete group {group_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

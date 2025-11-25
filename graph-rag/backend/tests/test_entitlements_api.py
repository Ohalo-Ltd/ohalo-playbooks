"""Unit tests for entitlements API endpoints."""

import pytest
from uuid import uuid4


class TestProjectUsersAPI:
    """Test project users endpoints."""

    @pytest.mark.asyncio
    async def test_create_user_success(self, client, test_project):
        """Test creating a user successfully."""
        response = await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Alice Johnson"
        assert data["email"] == "alice@example.com"
        assert data["role"] == "analyst"
        assert data["project_id"] == test_project["id"]
        assert "id" in data
        assert "created_at" in data
        assert "updated_at" in data

    @pytest.mark.asyncio
    async def test_create_user_duplicate_email(self, client, test_project):
        """Test creating a user with duplicate email fails."""
        # Create first user
        await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        
        # Try to create second user with same email
        response = await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Smith", "email": "alice@example.com", "role": "engineer"},
        )
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_list_users(self, client, test_project):
        """Test listing all users in a project."""
        # Create two users
        await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Bob Smith", "email": "bob@example.com", "role": "engineer"},
        )
        
        # List users
        response = await client.get(f"/api/projects/{test_project['id']}/users")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        emails = [user["email"] for user in data]
        assert "alice@example.com" in emails
        assert "bob@example.com" in emails

    @pytest.mark.asyncio
    async def test_get_user(self, client, test_project):
        """Test getting a specific user."""
        # Create user
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        user_id = create_response.json()["id"]
        
        # Get user
        response = await client.get(f"/api/projects/{test_project['id']}/users/{user_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == user_id
        assert data["email"] == "alice@example.com"

    @pytest.mark.asyncio
    async def test_get_user_not_found(self, client, test_project):
        """Test getting a non-existent user."""
        fake_id = str(uuid4())
        response = await client.get(f"/api/projects/{test_project['id']}/users/{fake_id}")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_update_user(self, client, test_project):
        """Test updating a user."""
        # Create user
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        user_id = create_response.json()["id"]
        
        # Update role
        response = await client.put(
            f"/api/projects/{test_project['id']}/users/{user_id}",
            json={"role": "senior_analyst"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["role"] == "senior_analyst"
        assert data["name"] == "Alice Johnson"  # Unchanged
        assert data["email"] == "alice@example.com"  # Unchanged

    @pytest.mark.asyncio
    async def test_update_user_multiple_fields(self, client, test_project):
        """Test updating multiple fields at once."""
        # Create user
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        user_id = create_response.json()["id"]
        
        # Update multiple fields
        response = await client.put(
            f"/api/projects/{test_project['id']}/users/{user_id}",
            json={"name": "Alice Smith", "role": "senior_analyst"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Alice Smith"
        assert data["role"] == "senior_analyst"
        assert data["email"] == "alice@example.com"  # Unchanged

    @pytest.mark.asyncio
    async def test_update_user_no_fields(self, client, test_project):
        """Test updating with no fields fails."""
        # Create user
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        user_id = create_response.json()["id"]
        
        # Try to update with no fields
        response = await client.put(
            f"/api/projects/{test_project['id']}/users/{user_id}",
            json={},
        )
        assert response.status_code == 400
        assert "No fields to update" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_delete_user(self, client, test_project):
        """Test deleting a user."""
        # Create user
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/users",
            json={"name": "Alice Johnson", "email": "alice@example.com", "role": "analyst"},
        )
        user_id = create_response.json()["id"]
        
        # Delete user
        response = await client.delete(f"/api/projects/{test_project['id']}/users/{user_id}")
        assert response.status_code == 204
        
        # Verify user is gone
        get_response = await client.get(f"/api/projects/{test_project['id']}/users/{user_id}")
        assert get_response.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_user_not_found(self, client, test_project):
        """Test deleting a non-existent user."""
        fake_id = str(uuid4())
        response = await client.delete(f"/api/projects/{test_project['id']}/users/{fake_id}")
        assert response.status_code == 404


class TestProjectGroupsAPI:
    """Test project groups endpoints."""

    @pytest.mark.asyncio
    async def test_create_group_success(self, client, test_project):
        """Test creating a group successfully."""
        response = await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Engineering Team"
        assert data["code"] == "ENG001"
        assert data["project_id"] == test_project["id"]
        assert "id" in data
        assert "created_at" in data
        assert "updated_at" in data

    @pytest.mark.asyncio
    async def test_create_group_duplicate_code(self, client, test_project):
        """Test creating a group with duplicate code fails."""
        # Create first group
        await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        
        # Try to create second group with same code
        response = await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Product Team", "code": "ENG001"},
        )
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_list_groups(self, client, test_project):
        """Test listing all groups in a project."""
        # Create two groups
        await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Product Team", "code": "PROD001"},
        )
        
        # List groups
        response = await client.get(f"/api/projects/{test_project['id']}/groups")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        codes = [group["code"] for group in data]
        assert "ENG001" in codes
        assert "PROD001" in codes

    @pytest.mark.asyncio
    async def test_get_group(self, client, test_project):
        """Test getting a specific group."""
        # Create group
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        group_id = create_response.json()["id"]
        
        # Get group
        response = await client.get(f"/api/projects/{test_project['id']}/groups/{group_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == group_id
        assert data["code"] == "ENG001"

    @pytest.mark.asyncio
    async def test_get_group_not_found(self, client, test_project):
        """Test getting a non-existent group."""
        fake_id = str(uuid4())
        response = await client.get(f"/api/projects/{test_project['id']}/groups/{fake_id}")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_update_group(self, client, test_project):
        """Test updating a group."""
        # Create group
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        group_id = create_response.json()["id"]
        
        # Update name
        response = await client.put(
            f"/api/projects/{test_project['id']}/groups/{group_id}",
            json={"name": "Senior Engineering Team"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Senior Engineering Team"
        assert data["code"] == "ENG001"  # Unchanged

    @pytest.mark.asyncio
    async def test_update_group_multiple_fields(self, client, test_project):
        """Test updating multiple fields at once."""
        # Create group
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        group_id = create_response.json()["id"]
        
        # Update multiple fields
        response = await client.put(
            f"/api/projects/{test_project['id']}/groups/{group_id}",
            json={"name": "Senior Engineering", "code": "SENG001"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Senior Engineering"
        assert data["code"] == "SENG001"

    @pytest.mark.asyncio
    async def test_update_group_no_fields(self, client, test_project):
        """Test updating with no fields fails."""
        # Create group
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        group_id = create_response.json()["id"]
        
        # Try to update with no fields
        response = await client.put(
            f"/api/projects/{test_project['id']}/groups/{group_id}",
            json={},
        )
        assert response.status_code == 400
        assert "No fields to update" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_delete_group(self, client, test_project):
        """Test deleting a group."""
        # Create group
        create_response = await client.post(
            f"/api/projects/{test_project['id']}/groups",
            json={"name": "Engineering Team", "code": "ENG001"},
        )
        group_id = create_response.json()["id"]
        
        # Delete group
        response = await client.delete(f"/api/projects/{test_project['id']}/groups/{group_id}")
        assert response.status_code == 204
        
        # Verify group is gone
        get_response = await client.get(f"/api/projects/{test_project['id']}/groups/{group_id}")
        assert get_response.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_group_not_found(self, client, test_project):
        """Test deleting a non-existent group."""
        fake_id = str(uuid4())
        response = await client.delete(f"/api/projects/{test_project['id']}/groups/{fake_id}")
        assert response.status_code == 404


class TestProjectEntitlementsEnabled:
    """Test entitlements_enabled field on projects."""

    @pytest.mark.asyncio
    async def test_create_project_entitlements_disabled_by_default(self, client):
        """Test that new projects have entitlements disabled by default."""
        response = await client.post(
            "/api/projects",
            json={"name": "Test Project"},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["entitlements_enabled"] is False

    @pytest.mark.asyncio
    async def test_create_project_with_entitlements_enabled(self, client):
        """Test creating a project with entitlements enabled."""
        response = await client.post(
            "/api/projects",
            json={"name": "Test Project", "entitlements_enabled": True},
        )
        assert response.status_code == 201
        data = response.json()
        assert data["entitlements_enabled"] is True

    @pytest.mark.asyncio
    async def test_update_project_enable_entitlements(self, client, test_project):
        """Test enabling entitlements on an existing project."""
        response = await client.patch(
            f"/api/projects/{test_project['id']}",
            json={"entitlements_enabled": True},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["entitlements_enabled"] is True

    @pytest.mark.asyncio
    async def test_list_projects_includes_entitlements_enabled(self, client, test_project):
        """Test that list projects includes entitlements_enabled field."""
        response = await client.get("/api/projects")
        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        # Check that every project has the entitlements_enabled field
        for project in data:
            assert "entitlements_enabled" in project
            assert isinstance(project["entitlements_enabled"], bool)

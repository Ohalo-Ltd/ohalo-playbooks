/**
 * API client for backend communication.
 */

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface QueryRequest {
  question: string;
  project_id?: string;
  top_k?: number;
  include_graph_context?: boolean;
  current_user_email?: string;
}

export interface SourceChunk {
  id: string;
  text: string;
  score: number;
  chunk_index: number;
}

export interface RelatedEntity {
  id: string;
  name: string;
  type: string;
}

export interface EntityRelationship {
  from_entity: string;
  to_entity: string;
  relationship_type: string;
}

export interface QueryResponse {
  answer: string;
  sources: SourceChunk[];
  related_entities: RelatedEntity[];
  relationships: EntityRelationship[];
  conversation_id: string;
}

export interface IngestionRequest {
  project_id: string;
  datasource_id: string;
  extractor_id?: string;
  max_documents?: number;
  fetch_content?: boolean;
}

export interface IngestionResponse {
  job_id: string;
  status: string;
  message: string;
}

export interface IngestionStatus {
  job_id: string;
  status: string;
  documents_processed: number;
  total_documents?: number;
  error?: string;
  started_at: string;
  completed_at?: string;
}

export interface GraphSchema {
  entity_types: Array<{
    type: string;
    count: number;
    properties: string[];
  }>;
  relationship_types: Array<{
    type: string;
    from_type: string;
    to_type: string;
    count: number;
  }>;
  statistics: Record<string, number>;
}

export interface Project {
  id: string;
  name: string;
  description?: string;
  system_prompt?: string;
  dxr_url?: string;
  dxr_api_token?: string;
  dxr_datasource_id?: string;
  dxr_extractor_id?: string;
  entitlements_enabled?: boolean;
  created_at: string;
  updated_at: string;
}

export interface ProjectCreate {
  name: string;
  description?: string;
  system_prompt?: string;
  dxr_url?: string;
  dxr_api_token?: string;
  dxr_datasource_id?: string;
  dxr_extractor_id?: string;
}

export interface ProjectUpdate {
  name?: string;
  description?: string;
  system_prompt?: string;
  dxr_url?: string;
  dxr_api_token?: string;
  dxr_datasource_id?: string;
  dxr_extractor_id?: string;
  entitlements_enabled?: boolean;
}

export interface ProjectUser {
  id: string;
  project_id: string;
  email: string;
  name: string;
  idp_id?: string;
  created_at: string;
  updated_at: string;
}

export interface ProjectUserCreate {
  email: string;
  name: string;
  idp_id?: string;
}

export interface ProjectUserUpdate {
  email?: string;
  name?: string;
  idp_id?: string;
}

export interface ProjectGroup {
  id: string;
  project_id: string;
  code: string;
  name: string;
  created_at: string;
  updated_at: string;
}

export interface ProjectGroupCreate {
  code: string;
  name: string;
}

export interface ProjectGroupUpdate {
  code?: string;
  name?: string;
}

export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  async query(request: QueryRequest): Promise<QueryResponse> {
    const response = await fetch(`${this.baseUrl}/api/chat/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Query failed');
    }

    return response.json();
  }

  async startIngestion(request: IngestionRequest): Promise<IngestionResponse> {
    const response = await fetch(`${this.baseUrl}/api/ingestion/start`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Ingestion failed');
    }

    return response.json();
  }

  async getIngestionStatus(jobId: string): Promise<IngestionStatus> {
    const response = await fetch(`${this.baseUrl}/api/ingestion/status/${jobId}`);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to get status');
    }

    return response.json();
  }

  async getSchema(): Promise<GraphSchema> {
    const response = await fetch(`${this.baseUrl}/api/chat/schema`);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to get schema');
    }

    return response.json();
  }

  async getSchemaSuggestions(): Promise<any> {
    const response = await fetch(`${this.baseUrl}/api/chat/schema/suggestions`);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to get suggestions');
    }

    return response.json();
  }

  // Project management
  async listProjects(): Promise<Project[]> {
    const response = await fetch(`${this.baseUrl}/api/projects`);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to list projects');
    }

    return response.json();
  }

  async getProject(projectId: string): Promise<Project> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}`);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to get project');
    }

    return response.json();
  }

  async createProject(project: ProjectCreate): Promise<Project> {
    const response = await fetch(`${this.baseUrl}/api/projects`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(project),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to create project');
    }

    return response.json();
  }

  async updateProject(projectId: string, updates: ProjectUpdate): Promise<Project> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(updates),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to update project');
    }

    return response.json();
  }

  async deleteProject(projectId: string): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}`, {
      method: 'DELETE',
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to delete project');
    }
  }

  async listDocuments(projectId: string): Promise<any[]> {
    const response = await fetch(`${this.baseUrl}/api/documents?project_id=${projectId}`);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to list documents');
    }

    return response.json();
  }

  // Entitlements - Users
  async listProjectUsers(projectId: string): Promise<ProjectUser[]> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/users`);
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to list users');
    }
    return response.json();
  }

  async createProjectUser(projectId: string, user: ProjectUserCreate): Promise<ProjectUser> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/users`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(user),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to create user');
    }
    return response.json();
  }

  async updateProjectUser(projectId: string, userId: string, updates: ProjectUserUpdate): Promise<ProjectUser> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/users/${userId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to update user');
    }
    return response.json();
  }

  async deleteProjectUser(projectId: string, userId: string): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/users/${userId}`, {
      method: 'DELETE',
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to delete user');
    }
  }

  // Entitlements - Groups
  async listProjectGroups(projectId: string): Promise<ProjectGroup[]> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/groups`);
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to list groups');
    }
    return response.json();
  }

  async createProjectGroup(projectId: string, group: ProjectGroupCreate): Promise<ProjectGroup> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/groups`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(group),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to create group');
    }
    return response.json();
  }

  async updateProjectGroup(projectId: string, groupId: string, updates: ProjectGroupUpdate): Promise<ProjectGroup> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/groups/${groupId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to update group');
    }
    return response.json();
  }

  async deleteProjectGroup(projectId: string, groupId: string): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/projects/${projectId}/entitlements/groups/${groupId}`, {
      method: 'DELETE',
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.detail || 'Failed to delete group');
    }
  }
}

export const apiClient = new ApiClient();

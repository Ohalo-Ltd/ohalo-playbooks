/**
 * API client for backend communication.
 */

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface QueryRequest {
  question: string;
  project_id?: string;
  top_k?: number;
  include_graph_context?: boolean;
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
  datasource_name: string;
  datasource_id?: string;
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
  created_at: string;
  updated_at: string;
}

export interface ProjectCreate {
  name: string;
  description?: string;
  system_prompt?: string;
}

export interface ProjectUpdate {
  name?: string;
  description?: string;
  system_prompt?: string;
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
}

export const apiClient = new ApiClient();

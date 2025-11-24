/**
 * API client for backend communication.
 */

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface QueryRequest {
  query: string;
  conversation_id?: string;
}

export interface QueryResponse {
  answer: string;
  conversation_id: string;
  sources: Array<{
    file_id: string;
    file_name: string;
    chunk_id: string;
    similarity: number;
    text: string;
  }>;
  entities_mentioned: string[];
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
}

export const apiClient = new ApiClient();

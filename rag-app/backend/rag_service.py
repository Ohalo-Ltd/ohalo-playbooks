"""
RAG service for streaming chat responses with pydantic-ai
"""

from typing import List, Dict, Any, AsyncGenerator
import json
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict
import asyncio
from core.logging import get_logger

import openai

logger = get_logger(__name__)
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.tools import RunContext

from core.config import Settings
from database import DatabaseManager


@dataclass
class ChatMessage:
    """Represents a chat message"""
    role: str  # 'user' or 'assistant'
    content: str


@dataclass
class ChatRequest:
    """Request for chat endpoint"""
    message: str
    user_email: str


class SearchDocumentsInput(BaseModel):
    query: str
    similarity_threshold: float


class QueryDecomposition(BaseModel):
    """Model for query decomposition results"""

    decomposed_queries: List[str]


class RAGService:
    """RAG service with streaming responses and in-memory chat memory, with tool-calling RAG"""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self.db_manager: DatabaseManager = DatabaseManager(self.settings)
        self.openai_client = openai.OpenAI(api_key=self.settings.openai_api_key)
        self.model = OpenAIModel('gpt-4o-mini')
        self.user_info = None  # Will be set per request/session

        # Create query decomposition agent
        self.query_decomposer = Agent(
            model=self.model,
            output_type=QueryDecomposition,
            system_prompt=self._get_query_decomposition_prompt(),
        )

        self.agent = Agent(
            model=self.model,
            system_prompt=self._get_system_prompt({}),
        )
        self.chat_memory = defaultdict(list)
        self.lock = asyncio.Lock()
        self._current_user_email = None  # For passing user_email to the tool

        @self.agent.tool
        async def search_documents_tool(
            context: RunContext, data: SearchDocumentsInput
        ) -> str:
            """
            Search company documents for relevant information using semantic similarity with query decomposition.
            - 'query': The user's search or question.
            - 'similarity_threshold': Controls how closely results must match the query (range 0.0-1.0).
                - Use a lower threshold (e.g., 0.01-0.2) for broad searches, when previous results were empty, or when the user asks for 'all' or 'any' documents.
                - Use a higher threshold (e.g., 0.5-0.8) for very specific, targeted questions.
            """
            user_email = self._current_user_email
            logger.debug(
                f"search_documents_tool called with query='{data.query}', user_email='{user_email}', similarity_threshold={data.similarity_threshold}"
            )
            if not user_email:
                return "User email not found in context."

            user_info = self._get_user_info(user_email)

            # Decompose the query into multiple related queries
            logger.debug(f"Decomposing query: {data.query}")
            decomposition = await self._decompose_query(data.query)
            logger.debug(f"Decomposed queries: {decomposition.decomposed_queries}")

            # Search with each decomposed query
            query_results = []
            for sub_query in decomposition.decomposed_queries:
                chunks = await self._retrieve_context(
                    sub_query,
                    similarity_threshold=data.similarity_threshold,
                    user_info=user_info,
                    limit=10,  # Get fewer results per query since we'll merge
                )
                query_results.append((sub_query, chunks))

            # Rank and merge results from all queries
            merged_chunks = self._rank_and_merge_results(query_results)
            logger.debug(
                f"Merged {len(merged_chunks)} chunks from {len(decomposition.decomposed_queries)} queries"
            )

            return self._format_context(merged_chunks)

        self.search_documents_tool = search_documents_tool

    def _get_query_decomposition_prompt(self) -> str:
        """Get the system prompt for query decomposition"""
        return """
        [ROLE]
        You are a query decomposition expert for an HR document search system.
        
        [SYSTEM KNOWLEDGE]
        - This is an HR assistant bot that searches company documents, including the documents that the user has authored or has access to.
        - Users ask questions about HR policies, performance reviews, paid/unpaid leave, benefits, etc.
        - The system uses semantic similarity search to find relevant document chunks
        
        [TASK]
        Break down the user's query into exactly 3 distinct, related search queries that will help find comprehensive information.
        Each query should:
        1. Target different aspects or angles of the original question
        2. Use different keywords and phrasings to capture diverse relevant content
        3. Be specific enough to find targeted information
        4. Cover the breadth of what the user might be looking for
        
        [EXAMPLES]
        Original: "What is our vacation policy?"
        Decomposed:
        1. "vacation time off policy annual leave"
        2. "paid time off PTO accrual rates"
        3. "holiday schedule vacation request approval process"
        
        Original: "Do we have any pet policies?"
        Decomposed:
        1. "pets in workplace office animals"
        2. "pet insurance benefits coverage"
        3. "service animals emotional support workplace accommodation"
        
        [OUTPUT]
        Return the 3 decomposed queries only and nothing else.
        """

    def _get_system_prompt(self, user_info: Dict[str, Any]) -> str:
        """Get the system prompt for the RAG agent, including user details if available"""
        user_info = user_info or {}
        name = user_info.get("name", "the user")
        email = user_info.get("email", "unknown")
        role = user_info.get("role", "unknown")
        employee_id = user_info.get("employee_id", "unknown")
        groups = ", ".join(user_info.get("groups", [])) or "none"

        user_details = f"""
        [USER]
        Name: {name}
        Email: {email}
        Role: {role}
        Employee ID: {employee_id}
        Groups: {groups}
        """

        return f"""
        [ROLE]
        You are a helpful HR assistant called PeoplePal, with access to company documents via a tool called search_documents.
        Always answer as a professional HR assistant, focusing on clarity, helpfulness, and accuracy.
        
        [SYSTEM KNOWLEDGE]
        Add current date and time to the context.
        Today is {datetime.today().strftime('%Y-%m-%d')}.

        {user_details}

        [BEHAVIOR]
        - For every user question, ALWAYS call the search_documents tool with the user's query, even if you think you know the answer.
        - The search_documents tool uses advanced query decomposition to break your search into 3 related queries and rank results for comprehensive coverage.
        - Example: If the user asks 'Do we have anything on pets?', you MUST respond by calling search_documents('Do we have anything on pets?') and wait for the results before answering.
        - After receiving the results from search_documents, use them to answer the user's question.
        - If the search returns no relevant documents, politely say so.
        - Be concise, avoid unnecessary fluff, and cite the document or type of document when possible.
        - Never answer from your own knowledge or make up information.

        [FORMATTING]
        - Format all responses using proper markdown suitable for rendering with react-markdown.
        - When including tables, always surround them with blank lines before and after, and use valid markdown table syntax.
        - Support bold, italic, headers, and tables as needed for clarity.
        - Use lists and bullet points where appropriate for readability.

        [STYLE]
        - Write in a clear, friendly, and personal tone.
        - Use the user's name when appropriate.
        - Be relaxed and approachable, while remaining professional.
        - Focus on providing direct, actionable answers to the user's question.
        """

    async def _get_embeddings(self, text: str) -> List[float]:
        """Get OpenAI embeddings for a text"""
        response = self.openai_client.embeddings.create(
            model="text-embedding-3-small", input=text
        )
        return response.data[0].embedding

    def _get_user_info(self, email: str) -> Dict[str, Any]:
        """Get user info from mock IdP - in reality this would validate JWT"""
        from auth.mock_idp import USER_PROFILES
        return USER_PROFILES.get(email, {})

    async def _decompose_query(self, query: str) -> QueryDecomposition:
        """Decompose a user query into multiple related queries"""
        try:
            result = await self.query_decomposer.run(
                f"Decompose this HR-related query: '{query}'"
            )
            return result.data
        except Exception as e:
            logger.error(f"Query decomposition failed: {e}")
            # Fallback to original query
            return QueryDecomposition(
                decomposed_queries=[query, query, query],
            )

    def _rank_and_merge_results(
        self,
        query_results: List[tuple[str, List[Dict[str, Any]]]],
        max_results: int = 15,
    ) -> List[Dict[str, Any]]:
        """Rank and merge results from multiple decomposed queries"""
        # Collect all unique chunks with their scores and source queries
        chunk_scores = {}  # chunk_id -> {chunk: dict, scores: list, queries: list}

        for query, chunks in query_results:
            for i, chunk in enumerate(chunks):
                # Create a unique identifier for the chunk
                chunk_id = f"{chunk['title']}_{chunk.get('chunk_index', 0)}"

                # Calculate a relevance score (higher rank = lower score)
                relevance_score = 1.0 / (
                    i + 1
                )  # First result gets 1.0, second gets 0.5, etc.

                if chunk_id not in chunk_scores:
                    chunk_scores[chunk_id] = {
                        "chunk": chunk,
                        "scores": [],
                        "queries": [],
                    }

                chunk_scores[chunk_id]["scores"].append(relevance_score)
                chunk_scores[chunk_id]["queries"].append(query)

        # Calculate final scores using RRF (Reciprocal Rank Fusion)
        final_chunks = []
        for chunk_id, data in chunk_scores.items():
            # RRF score: sum of 1/(rank + k) for each query, where k=60 is common
            rrf_score = sum(score for score in data["scores"])

            chunk_with_score = data["chunk"].copy()
            chunk_with_score["_rrf_score"] = rrf_score
            chunk_with_score["_matching_queries"] = data["queries"]
            final_chunks.append(chunk_with_score)

        # Sort by RRF score (descending) and limit results
        final_chunks.sort(key=lambda x: x["_rrf_score"], reverse=True)
        return final_chunks[:max_results]

    async def _retrieve_context(
        self,
        query: str,
        user_info: Dict[str, Any],
        similarity_threshold: float,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Retrieve relevant document chunks for the query"""
        query_embedding = await self._get_embeddings(query)

        # Use database manager to search with RLS
        return self.db_manager.search_chunks(
            query_embedding, user_info, limit, similarity_threshold
        )

    def _format_context(self, chunks: List[Dict[str, Any]]) -> str:
        """Format retrieved chunks into context for the LLM"""
        if not chunks:
            return "No relevant documents found for this query."

        # For regular queries with RRF-ranked results
        context_parts = []
        doc_chunk_count = {}
        chunks_added = 0
        max_chunks_per_doc = 3

        for chunk in chunks:
            doc_title = chunk["title"]
            current_count = doc_chunk_count.get(doc_title, 0)

            if current_count >= max_chunks_per_doc:
                continue

            doc_chunk_count[doc_title] = current_count + 1
            chunks_added += 1

            # Include RRF score information if available
            rrf_info = ""
            if "_rrf_score" in chunk:
                rrf_info = f" (relevance: {chunk['_rrf_score']:.3f})"

            matching_queries_info = ""
            if "_matching_queries" in chunk:
                matching_queries_info = (
                    f"\n   Matched queries: {', '.join(chunk['_matching_queries'])}"
                )

            context_parts.append(
                f"Document {chunks_added} (from {chunk['title']}, category: {chunk['category']}){rrf_info}:{matching_queries_info}\n{chunk['content']}\n"
            )

            if chunks_added >= 15:  # Limit total chunks
                break

        return "\n---\n".join(context_parts)

    async def chat_stream(self, request: ChatRequest) -> AsyncGenerator[str, None]:
        """Stream chat response using RAG with in-memory memory and tool-calling"""
        try:
            user_info = self._get_user_info(request.user_email)
            if not user_info:
                yield json.dumps({"error": "Invalid user credentials"}) + "\n"
                return

            async with self.lock:
                history = list(self.chat_memory[request.user_email][-20:])
                history.append({"role": "user", "content": request.message})

            # Format chat history as a string
            history_str = "\n".join(
                [f"{msg['role'].capitalize()}: {msg['content']}" for msg in history]
            )
            prompt = (
                f"{self._get_system_prompt(user_info)}\n"
                f"Chat history (use this to understand the conversation context):\n{history_str}\n"
                f"Remember: For every user question, you must call search_documents(query='<user question>') before answering.\n"
                f"Assistant:"
            )

            # Set the current user email for the tool
            self._current_user_email = request.user_email
            async with self.agent.run_stream(prompt) as result:
                async for chunk in result.stream():
                    yield f"data: {json.dumps({'content': chunk})}\n\n"
                    async with self.lock:
                        self.chat_memory[request.user_email].append(
                            {"role": "user", "content": request.message}
                        )
                        self.chat_memory[request.user_email].append(
                            {"role": "assistant", "content": chunk}
                        )
            self._current_user_email = None

            yield f"data: {json.dumps({'done': True})}\n\n"

        except Exception as e:
            logger.error(f"chat_stream exception: {e}")
            error_msg = f"Error processing request: {str(e)}"
            yield f"data: {json.dumps({'error': error_msg})}\n\n"

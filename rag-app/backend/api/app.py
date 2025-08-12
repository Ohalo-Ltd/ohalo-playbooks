"""
API package - FastAPI endpoints with Row Level Security
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Dict, Any

from core.config import Settings
from auth.mock_idp import router as idp_router, USER_PROFILES
from database import DatabaseManager
from api.middleware import SecurityHeadersMiddleware
from rag_service import RAGService, ChatRequest

# Initialize settings and database manager
settings = Settings()
db_manager = DatabaseManager(settings)
rag_service = RAGService(settings)

app = FastAPI(
    title="RAG App API",
    description="Secure RAG system with Row Level Security and Mock IdP",
    version="0.3.0",
)

# Add middleware
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(idp_router, tags=["Identity Provider"])


def get_user_info(email: str = Query(..., description="User email for authentication")) -> Dict[str, Any]:
    """
    Dependency to get user info from mock IdP
    In a real system, this would validate JWT tokens
    """
    user_profile = USER_PROFILES.get(email)
    if not user_profile:
        raise HTTPException(status_code=401, detail="Invalid user credentials")
    return user_profile


@app.get("/")
async def root():
    return {
        "message": "RAG App API - Row Level Security Demo Ready",
        "version": "0.3.0",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    from datetime import datetime

    return {
        "status": "healthy",
        "version": "0.3.0",
        "timestamp": datetime.now().isoformat(),
        "environment": settings.environment,
    }


@app.get("/api/documents")
async def get_documents(user_info: Dict[str, Any] = Depends(get_user_info)):
    """Get documents accessible to the current user"""
    try:
        documents = db_manager.get_documents_for_user(user_info)
        return {"documents": documents}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch documents: {str(e)}")


class ChatMessageRequest(BaseModel):
    """Request model for chat messages"""

    message: str


@app.post("/chat")
async def chat_stream(
    chat_request: ChatMessageRequest, user_info: Dict[str, Any] = Depends(get_user_info)
):
    """Stream chat responses using RAG"""
    try:
        request = ChatRequest(
            message=chat_request.message, user_email=user_info["email"]
        )

        return StreamingResponse(
            rag_service.chat_stream(request),
            media_type="text/plain",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Content-Type": "text/event-stream",
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chat error: {str(e)}")
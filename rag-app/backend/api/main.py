"""
Main FastAPI application entry point
"""

from api import app, settings

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.backend_port)

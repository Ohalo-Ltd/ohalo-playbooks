"""
Data ingestion package - handles document import from DXR with superuser privileges
"""

from .config import IngestionSettings
from .database import IngestionDatabaseManager
from .processor import DocumentProcessor
from .cli import main as run_ingestion

__all__ = ["IngestionSettings", "IngestionDatabaseManager", "DocumentProcessor", "run_ingestion"]

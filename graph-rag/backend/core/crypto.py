"""Encryption utilities for sensitive data."""

from core.config import settings


def get_encryption_key() -> str:
    """Get the encryption key from settings.
    
    Returns:
        Encryption key for pgcrypto
    """
    return settings.secret_key


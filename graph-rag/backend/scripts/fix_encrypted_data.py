"""Fix encrypted data in projects table.

This script handles the case where encryption key has changed and existing
encrypted data cannot be decrypted. It will set encrypted fields to NULL
so they can be re-entered.
"""

import asyncio
import logging

from database.postgres_client import PostgresClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def fix_encrypted_data():
    """Set encrypted fields to NULL for projects that can't be decrypted."""
    pg_client = PostgresClient()
    await pg_client.connect()

    try:
        # Update all projects to set encrypted token to NULL
        # This allows the application to work, users can re-enter their tokens
        result = await pg_client.execute(
            """
            UPDATE projects 
            SET dxr_api_token = NULL
            WHERE dxr_api_token IS NOT NULL
            """
        )

        logger.info(f"Reset encrypted tokens for projects. Result: {result}")
        logger.info(
            "Users will need to re-enter their DXR API tokens in project settings."
        )

    except Exception as e:
        logger.error(f"Failed to fix encrypted data: {e}")
        raise
    finally:
        await pg_client.close()


if __name__ == "__main__":
    asyncio.run(fix_encrypted_data())

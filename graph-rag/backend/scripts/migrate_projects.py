import asyncio
from database.postgres_client import PostgresClient

async def migrate():
    print("Migrating projects table...")
    client = PostgresClient()
    await client.connect()

    try:
        # Ensure pgcrypto extension is enabled
        await client.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        print("Ensured pgcrypto extension is enabled")

        # Change dxr_api_token to BYTEA to support encrypted data
        try:
            await client.execute(
                "ALTER TABLE projects ALTER COLUMN dxr_api_token TYPE BYTEA USING dxr_api_token::bytea"
            )
            print("Changed dxr_api_token column type to BYTEA")
        except Exception as e:
            print(f"Note: dxr_api_token column type: {e}")

        # Check if columns exist
        columns = [
            "dxr_url",
            "dxr_api_token",
            "dxr_datasource_id",
            "dxr_extractor_id"
        ]

        for col in columns:
            try:
                col_type = "BYTEA" if col == "dxr_api_token" else "VARCHAR(255)"
                await client.execute(
                    f"ALTER TABLE projects ADD COLUMN IF NOT EXISTS {col} {col_type}"
                )
                print(f"Added column {col}")
            except Exception as e:
                print(f"Error adding column {col}: {e}")

    finally:
        await client.close()
    print("Migration complete.")

if __name__ == "__main__":
    asyncio.run(migrate())

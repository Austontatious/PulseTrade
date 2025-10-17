import os
import pytest
import asyncpg

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

@pytest.mark.asyncio
async def test_tables_exist():
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        for table in ["trades", "forecasts", "social_messages", "analyst_ratings"]:
            result = await conn.fetchval("SELECT to_regclass($1)", table)
            assert result == table
    finally:
        await conn.close()

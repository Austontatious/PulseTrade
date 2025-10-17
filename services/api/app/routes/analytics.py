from fastapi import APIRouter
import asyncpg
import os

router = APIRouter()

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

@router.get("/recent")
async def recent_activity(ticker: str):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        social = await conn.fetch(
            """SELECT ts, source, handle, text FROM social_messages
            WHERE ticker=$1 ORDER BY ts DESC LIMIT 50""",
            ticker,
        )
        ratings = await conn.fetch(
            """SELECT ts, provider, firm, analyst, action, rating, target FROM analyst_ratings
            WHERE ticker=$1 ORDER BY ts DESC LIMIT 50""",
            ticker,
        )
        return {
            "social": [dict(row) for row in social],
            "ratings": [dict(row) for row in ratings],
        }
    finally:
        await conn.close()

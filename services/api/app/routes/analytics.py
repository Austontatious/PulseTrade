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

@router.get("/fills")
async def recent_fills(ticker: str | None = None, limit: int = 50):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        if ticker:
            rows = await conn.fetch(
                """SELECT ts, ticker, side, qty, price, venue, meta
                    FROM fills
                    WHERE ticker=$1
                    ORDER BY ts DESC
                    LIMIT $2""",
                ticker,
                limit,
            )
        else:
            rows = await conn.fetch(
                """SELECT ts, ticker, side, qty, price, venue, meta
                    FROM fills
                    ORDER BY ts DESC
                    LIMIT $1""",
                limit,
            )
        return [dict(row) for row in rows]
    finally:
        await conn.close()

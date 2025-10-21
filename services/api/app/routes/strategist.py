from fastapi import APIRouter, Query
import asyncpg, os
from typing import List

router = APIRouter()

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

@router.get("/recent")
async def recent_recos(ticker: str | None = None, limit: int = 50):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        if ticker:
            rows = await conn.fetch(
                """
                SELECT ts, ticker, side, score, horizon, reason, meta
                FROM strategist_recos
                WHERE ticker=$1
                ORDER BY ts DESC
                LIMIT $2
                """,
                ticker,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT ts, ticker, side, score, horizon, reason, meta
                FROM strategist_recos
                ORDER BY ts DESC
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]
    finally:
        await conn.close()

@router.get("/knobs")
async def active_knobs(limit: int = 100):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        rows = await conn.fetch(
            """
            SELECT ts, key, value, expires_at
            FROM policy_knobs
            WHERE (expires_at IS NULL OR expires_at > NOW())
            ORDER BY ts DESC
            LIMIT $1
            """,
            limit,
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import asyncpg
from fastapi import APIRouter

from libs.llm.run_and_log import LLMRunner

router = APIRouter()

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

_runner = LLMRunner()


async def _summaries(ticker: str, items: list[str]) -> str:
    if not items:
        return ""
    loop = asyncio.get_running_loop()
    as_of = datetime.now(timezone.utc).isoformat()
    try:
        result = await loop.run_in_executor(
            None,
            lambda: _runner.cached_call(
                "summary",
                {
                    "ticker": ticker,
                    "items": "\n".join(items),
                    "as_of": as_of,
                },
            ),
        )
    except Exception:
        return ""
    return result.get("text", "") or ""

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
            "summary": await _summaries(
                ticker,
                [
                    f"{row['ts']:%Y-%m-%d %H:%M} | {row['source']}: {row['text']}"
                    for row in social
                ]
                + [
                    f"{row['ts']:%Y-%m-%d %H:%M} | {row['provider']} {row['action']} {row['rating']}"
                    for row in ratings
                ],
            ),
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

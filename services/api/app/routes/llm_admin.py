from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from functools import partial

import asyncpg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from libs.llm.run_and_log import LLMRunner
from libs.llm.schemas import POLICY_SCHEMA


router = APIRouter(prefix="/llm/admin", tags=["llm-admin"])

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

runner = LLMRunner()
SCHEMA_MAP = {"policy": POLICY_SCHEMA}


class ReplayRequest(BaseModel):
    id: int
    force: bool = False


async def _fetch_call(call_id: int):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        row = await conn.fetchrow("SELECT * FROM llm_calls WHERE id=$1", call_id)
    finally:
        await conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Call not found")
    return dict(row)


@router.get("/call/{call_id}")
async def get_call(call_id: int):
    row = await _fetch_call(call_id)
    return row


@router.post("/replay")
async def replay(req: ReplayRequest):
    row = await _fetch_call(req.id)
    tpl_vars = json.loads(row["input_payload"])
    schema = SCHEMA_MAP.get(row["prompt_key"])
    result = await _call_runner(
        row["prompt_key"],
        tpl_vars,
        version=row["prompt_version"],
        schema=schema,
        use_cache=not req.force,
    )
    return result


@router.get("/stats")
async def stats(hours: int = 24):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        totals = await conn.fetchrow(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN success THEN 1 ELSE 0 END) AS success_count,
                   SUM(CASE WHEN success THEN 0 ELSE 1 END) AS failure_count,
                   AVG(latency_ms) AS avg_latency
            FROM llm_calls
            WHERE ts >= $1
            """,
            cutoff,
        )
        by_prompt = await conn.fetch(
            """
            SELECT prompt_key, prompt_version, success, COUNT(*) AS n
            FROM llm_calls
            WHERE ts >= $1
            GROUP BY prompt_key, prompt_version, success
            ORDER BY prompt_key, prompt_version, success DESC
            """,
            cutoff,
        )
    finally:
        await conn.close()
    return {
        "since": cutoff.isoformat(),
        "totals": dict(totals) if totals else {},
        "breakdown": [dict(row) for row in by_prompt],
    }


async def _call_runner(prompt_key: str, tpl_vars: dict[str, str], **kwargs):
    func = partial(runner.cached_call, prompt_key, tpl_vars, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func)

from __future__ import annotations

import os
from typing import List

import asyncpg
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

try:  # pragma: no cover - optional dependency when services package not packaged with API image
    from services.policy.policy.executor import close_position, get_all_positions  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    close_position = None  # type: ignore
    get_all_positions = None  # type: ignore

router = APIRouter()

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)


class PanicExitRequest(BaseModel):
    ttl_minutes: int = 60
    reason: str = "panic_exit"

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


@router.post("/panic-exit")
async def panic_exit(req: PanicExitRequest):
    if os.getenv("ENABLE_PANIC_EXIT", "0") != "1":
        raise HTTPException(status_code=403, detail="panic exit disabled")

    if close_position is None or get_all_positions is None:
        raise HTTPException(status_code=503, detail="panic exit helpers unavailable in this build")

    ttl = max(1, req.ttl_minutes)
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(
            """
            INSERT INTO circuit_breakers(ts, scope, key, active, reason, expires_at, meta)
            VALUES (NOW(), 'global', 'ALL', TRUE, $1, NOW() + make_interval(mins => $2::int), '{}')
            """,
            req.reason,
            ttl,
        )
    finally:
        await conn.close()

    # Attempt to close all positions via Alpaca
    try:
        positions = await get_all_positions()
    except Exception as exc:  # pragma: no cover - network failure
        raise HTTPException(status_code=502, detail=f"position fetch failed: {exc}") from exc

    closed = 0
    for pos in positions:
        symbol = (pos.get("symbol") or "").upper()
        if not symbol:
            continue
        try:
            await close_position(symbol)
            closed += 1
        except Exception:
            continue

    return {"ok": True, "positions_closed": closed, "ttl_minutes": ttl}

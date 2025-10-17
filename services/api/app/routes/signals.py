from fastapi import APIRouter, Query
import asyncpg, os
from typing import List

router = APIRouter()

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

@router.get("/latest")
async def latest_signals(tickers: List[str] = Query(default=[]), horizon: str = "1m"):
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        if not tickers:
            q = """SELECT DISTINCT ON (ticker) ts, ticker, horizon, model, mean, lower, upper FROM forecasts
                  WHERE horizon=$1 ORDER BY ticker, ts DESC LIMIT 100"""
            rows = await conn.fetch(q, horizon)
        else:
            q = """SELECT DISTINCT ON (ticker) ts, ticker, horizon, model, mean, lower, upper FROM forecasts
                  WHERE horizon=$1 AND ticker = ANY($2) ORDER BY ticker, ts DESC"""
            rows = await conn.fetch(q, horizon, tickers)
        return [dict(r) for r in rows]
    finally:
        await conn.close()

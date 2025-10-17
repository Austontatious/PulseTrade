from fastapi import APIRouter
import asyncpg
import os

router = APIRouter()

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

@router.get("/health")
def health():
    return {"status": "ok"}

@router.get("/lag")
async def lag():
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        ts = await conn.fetchval("SELECT MAX(ts) FROM trades")
        return {"latest_trade_ts": ts.isoformat() if ts else None}
    finally:
        await conn.close()

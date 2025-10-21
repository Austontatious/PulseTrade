import asyncpg
import os

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

async def ensure_tables() -> None:
    ddl_recos = """
    CREATE TABLE IF NOT EXISTS strategist_recos (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ NOT NULL,
      ticker TEXT NOT NULL,
      side TEXT NOT NULL,
      score DOUBLE PRECISION NOT NULL,
      horizon TEXT,
      reason TEXT,
      meta JSONB DEFAULT '{}'::jsonb
    );
    """
    ddl_knobs = """
    CREATE TABLE IF NOT EXISTS policy_knobs (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ NOT NULL,
      key TEXT NOT NULL,
      value JSONB NOT NULL,
      expires_at TIMESTAMPTZ,
      meta JSONB DEFAULT '{}'::jsonb
    );
    """
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(ddl_recos)
        await conn.execute(ddl_knobs)
    finally:
        await conn.close()


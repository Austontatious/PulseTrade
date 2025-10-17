from __future__ import annotations
import os, time, json, asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict
import asyncpg
from prometheus_client import Counter, Gauge, Histogram

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

EVENT_COUNTER = Counter("pulse_events_total", "Events by type", ["type"])
RATE_COUNTER  = Counter("pulse_rate_events_total", "Rate/backoff events", ["source","kind"])
LATENCY_HIST  = Histogram("pulse_latency_seconds", "Latency buckets", ["op"])

@asynccontextmanager
async def pg_pool():
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10)
    try:
        yield pool
    finally:
        await pool.close()

def rate_note(source: str, kind: str, meta: Dict[str,Any] | None=None):
    RATE_COUNTER.labels(source, kind).inc()

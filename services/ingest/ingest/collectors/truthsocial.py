import os
import datetime as dt
import asyncpg
from common.http_helpers import RateLimiter, get_json
from ..config import DB_DSN

ENDPOINT = os.getenv("TRUTH_SOCIAL_SCRAPER_URL")
RPS = float(os.getenv("RATE_MAX_HTTP_PER_SEC", "1"))
limiter = RateLimiter(RPS)

async def fetch_latest() -> None:
    if not ENDPOINT:
        return
    await limiter.wait()
    data = await get_json(ENDPOINT)
    if not isinstance(data, list):
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        for item in data:
            ts_raw = item.get("ts")
            if not ts_raw:
                continue
            ts = dt.datetime.fromisoformat(ts_raw).astimezone(dt.timezone.utc)
            await conn.execute(
                """
                INSERT INTO social_messages(ts,source,handle,ticker,text,sentiment,engagement,meta)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                ts,
                "truthsocial",
                item.get("handle"),
                item.get("ticker"),
                item.get("text"),
                item.get("sentiment"),
                int(item.get("engagement", 0) or 0),
                item,
            )
    finally:
        await conn.close()

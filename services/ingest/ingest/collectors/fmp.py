import os
import datetime as dt
import asyncpg
from common.http_helpers import RateLimiter, get_json
from ..config import DB_DSN

FMP_KEY = os.getenv("FMP_API_KEY")
RPS = float(os.getenv("RATE_MAX_HTTP_PER_SEC", "5"))
limiter = RateLimiter(RPS)
BASE = "https://financialmodelingprep.com/api/v3"

async def fetch_targets(ticker: str) -> None:
    if not FMP_KEY:
        return
    await limiter.wait()
    url = f"{BASE}/price-target/{ticker}"
    data = await get_json(url, params={"apikey": FMP_KEY})
    if not isinstance(data, list):
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        for row in data:
            published = row.get("publishedDate")
            if published:
                ts = dt.datetime.fromisoformat(published).replace(tzinfo=dt.timezone.utc)
            else:
                ts = dt.datetime.now(dt.timezone.utc)
            await conn.execute(
                """
                INSERT INTO analyst_ratings(ts,ticker,provider,firm,analyst,action,rating,target,meta)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                """,
                ts,
                ticker,
                "fmp",
                row.get("analystCompany"),
                row.get("analystName"),
                row.get("action") or "maintain",
                str(row.get("rating") or ""),
                float(row.get("priceTarget") or 0.0),
                row,
            )
    finally:
        await conn.close()

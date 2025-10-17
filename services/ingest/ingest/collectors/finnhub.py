import os
import datetime as dt
import asyncpg
from common.http_helpers import RateLimiter, get_json
from ..config import DB_DSN

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")
RPS = float(os.getenv("RATE_MAX_HTTP_PER_SEC", "5"))
limiter = RateLimiter(RPS)
BASE = "https://finnhub.io/api/v1/stock/recommendation"

async def fetch_and_store(ticker: str) -> None:
    if not FINNHUB_KEY:
        return
    await limiter.wait()
    data = await get_json(BASE, params={"symbol": ticker, "token": FINNHUB_KEY})
    if not isinstance(data, list):
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        for row in data:
            period = row.get("period")
            if not period:
                continue
            if len(period) == 7:
                period = f"{period}-01"
            ts = dt.datetime.fromisoformat(period).replace(tzinfo=dt.timezone.utc)
            rating = (
                f"sb:{row.get('strongBuy', 0)} "
                f"b:{row.get('buy', 0)} "
                f"h:{row.get('hold', 0)} "
                f"s:{row.get('sell', 0)} "
                f"ss:{row.get('strongSell', 0)}"
            )
            await conn.execute(
                """
                INSERT INTO analyst_ratings(ts,ticker,provider,firm,analyst,action,rating,target,meta)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT DO NOTHING
                """,
                ts,
                ticker,
                "finnhub",
                None,
                None,
                "maintain",
                rating,
                None,
                row,
            )
    finally:
        await conn.close()

import os
import datetime as dt
import asyncpg
from common.http_helpers import RateLimiter, get_json
from ..config import DB_DSN

API_KEY = os.getenv("QUIVER_API_KEY")
RPS = float(os.getenv("RATE_MAX_HTTP_PER_SEC", "2"))
limiter = RateLimiter(RPS)
BASE = os.getenv("QUIVER_BASE", "https://api.quiverquant.com/beta")

async def fetch_congress_trades(ticker: str) -> None:
    if not API_KEY:
        return
    await limiter.wait()
    headers = {"Authorization": f"Token {API_KEY}"}
    url = f"{BASE}/historical/congresstrading/transactions"
    data = await get_json(url, headers=headers, params={"tickers": ticker})
    if not isinstance(data, list):
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        for row in data:
            trade_date = row.get("TransactionDate")
            if not trade_date:
                continue
            ts = dt.datetime.fromisoformat(f"{trade_date}T00:00:00+00:00")
            text = f"{row.get('Representative')} {row.get('Transaction')} {row.get('Ticker')} ${row.get('Amount')}"
            await conn.execute(
                """
                INSERT INTO social_messages(ts,source,handle,ticker,text,sentiment,engagement,meta)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                ts,
                "quiver",
                row.get("Representative"),
                row.get("Ticker"),
                text,
                None,
                0,
                row,
            )
    finally:
        await conn.close()

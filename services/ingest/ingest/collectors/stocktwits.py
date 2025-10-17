import os
import datetime as dt
import asyncpg
from common.http_helpers import RateLimiter, get_json
from ..config import DB_DSN

RPS = float(os.getenv("RATE_MAX_HTTP_PER_SEC", "2"))
limiter = RateLimiter(RPS)
BASE = "https://api.stocktwits.com/api/2/streams/symbol/{sym}.json"

async def fetch_symbol(symbol: str) -> None:
    await limiter.wait()
    data = await get_json(BASE.format(sym=symbol))
    messages = data.get("messages", []) if isinstance(data, dict) else []
    if not messages:
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        for msg in messages:
            created = msg.get("created_at")
            if not created:
                continue
            ts = dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
            body = msg.get("body", "")
            user = msg.get("user", {}).get("username")
            engagement = int((msg.get("reshares") or 0) + (msg.get("reply_count") or 0))
            await conn.execute(
                """
                INSERT INTO social_messages(ts,source,handle,ticker,text,sentiment,engagement,meta)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                ts,
                "stocktwits",
                user,
                symbol.upper(),
                body,
                None,
                engagement,
                msg,
            )
    finally:
        await conn.close()

import os
import datetime as dt
import asyncpg
import httpx
from bs4 import BeautifulSoup
from ..config import DB_DSN

URL = os.getenv("CAPITOLTRADES_LATEST_URL", "https://www.capitoltrades.com/trades?sortBy=disclosureDate")

async def fetch_latest(limit: int = 50) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.select("[data-cy='trade-card']")[:limit]
    if not cards:
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        for card in cards:
            rep_node = card.select_one("[data-cy='politician-name']")
            ticker_node = card.select_one("[data-cy='ticker']")
            action_node = card.select_one("[data-cy='transaction-type']")
            date_node = card.select_one("[data-cy='transaction-date']")
            rep = rep_node.get_text(strip=True) if rep_node else None
            ticker = ticker_node.get_text(strip=True) if ticker_node else None
            action = action_node.get_text(strip=True) if action_node else None
            date_text = date_node.get_text(strip=True) if date_node else None
            ts = dt.datetime.now(dt.timezone.utc)
            text = f"{rep} {action} {ticker} on {date_text}"
            await conn.execute(
                """
                INSERT INTO social_messages(ts,source,handle,ticker,text,sentiment,engagement,meta)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                """,
                ts,
                "capitoltrades",
                rep,
                ticker,
                text,
                None,
                0,
                {"date": date_text},
            )
    finally:
        await conn.close()

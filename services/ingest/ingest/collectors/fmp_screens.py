import os
import asyncio
from typing import List
import httpx
import asyncpg
from ..config import DB_DSN

FMP_KEY = os.getenv("FMP_API_KEY")

ENDPOINTS = [
    "https://financialmodelingprep.com/api/v3/stock_market/actives",
    "https://financialmodelingprep.com/api/v3/stock_market/gainers",
    "https://financialmodelingprep.com/api/v3/stock_market/losers",
]

def _clean(sym: str) -> str:
    return sym.strip().upper().replace(".", "-")

async def fetch_screens() -> None:
    if not FMP_KEY:
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            tickers: List[str] = []
            for url in ENDPOINTS:
                try:
                    r = await client.get(url, params={"apikey": FMP_KEY})
                    if r.status_code != 200:
                        continue
                    data = r.json()
                    for item in data:
                        sym = _clean(item.get("symbol", ""))
                        if sym and " " not in sym:
                            tickers.append(sym)
                except Exception:
                    continue
            if tickers:
                rows = [(t, 'equity', 'ALPACA') for t in list(dict.fromkeys(tickers))]
                await conn.executemany(
                    "INSERT INTO symbols(ticker, class, venue) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                    rows,
                )
    finally:
        await conn.close()


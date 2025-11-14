import os
import asyncpg
import httpx
from ..config import DB_DSN

API_KEY = os.getenv("FMP_API_KEY")
BASE = "https://financialmodelingprep.com/api/v3"

DDL = """
CREATE TABLE IF NOT EXISTS dim_company_profile (
  symbol TEXT PRIMARY KEY,
  name TEXT,
  sector TEXT,
  industry TEXT,
  country TEXT,
  meta JSONB DEFAULT '{}'::jsonb
);
"""

async def fetch_profile(ticker: str) -> None:
    if not API_KEY:
        return
    url = f"{BASE}/profile/{ticker}"
    params = {"apikey": API_KEY}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return
        data = r.json()
        if not isinstance(data, list) or not data:
            return
        item = data[0]
    name = item.get("companyName") or item.get("company") or item.get("symbol")
    sector = item.get("sector")
    industry = item.get("industry")
    country = item.get("country")
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(DDL)
        await conn.execute(
            """
            INSERT INTO dim_company_profile(symbol, name, sector, industry, country, meta)
            VALUES($1,$2,$3,$4,$5,$6)
            ON CONFLICT (symbol) DO UPDATE SET
              name=EXCLUDED.name,
              sector=EXCLUDED.sector,
              industry=EXCLUDED.industry,
              country=EXCLUDED.country,
              meta=COALESCE(dim_company_profile.meta, '{}'::jsonb)
            """,
            ticker,
            name,
            sector,
            industry,
            country,
            item,
        )
    finally:
        await conn.close()


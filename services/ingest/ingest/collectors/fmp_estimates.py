import os
import datetime as dt
import asyncpg
import httpx
from ..config import DB_DSN

API_KEY = os.getenv("FMP_API_KEY")
BASE = "https://financialmodelingprep.com/api/v3"

DDL = """
CREATE TABLE IF NOT EXISTS fmp_analyst_estimates (
  symbol TEXT NOT NULL,
  date DATE NOT NULL,
  payload JSONB NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, date)
);
"""

async def fetch_estimates(ticker: str) -> None:
    if not API_KEY:
        return
    url = f"{BASE}/analyst-estimates/{ticker}"
    params = {"apikey": API_KEY}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return
        data = r.json()
        if not isinstance(data, list):
            return
    rows = []
    for item in data:
        d = item.get("date")
        try:
            ds = dt.date.fromisoformat(d[:10]) if d else None
        except Exception:
            ds = None
        if not ds:
            continue
        rows.append((ticker, ds, item))
    if not rows:
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(DDL)
        await conn.executemany(
            """
            INSERT INTO fmp_analyst_estimates(symbol, date, payload)
            VALUES($1,$2,$3)
            ON CONFLICT (symbol, date) DO UPDATE SET payload=EXCLUDED.payload, ts=now()
            """,
            rows,
        )
    finally:
        await conn.close()


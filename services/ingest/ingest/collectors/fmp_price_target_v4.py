import datetime as dt
import os
from typing import Any, Dict, List

import asyncpg
import httpx

from ..config import DB_DSN

API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v4/price-target"
MAX_RECORDS = int(os.getenv("FMP_PRICE_TARGET_LIMIT", "200"))

DDL = """
CREATE TABLE IF NOT EXISTS fmp_price_targets_v4 (
  symbol TEXT NOT NULL,
  published_date DATE NOT NULL,
  target_high DOUBLE PRECISION,
  target_low DOUBLE PRECISION,
  target_avg DOUBLE PRECISION,
  target_current DOUBLE PRECISION,
  analyst_count INTEGER,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(symbol, published_date)
);
"""


def _date_from(value: Any) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except Exception:
        return None


async def fetch_price_targets(symbol: str) -> None:
    if not API_KEY:
        return
    params: Dict[str, Any] = {"symbol": symbol, "limit": MAX_RECORDS, "apikey": API_KEY}
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(BASE_URL, params=params)
    if resp.status_code != 200:
        return
    try:
        payload = resp.json()
    except Exception:
        return
    if not isinstance(payload, list):
        return
    rows: List[tuple] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        published = _date_from(item.get("publishedDate") or item.get("createdAt"))
        if not published:
            continue
        rows.append(
            (
                symbol,
                published,
                float(item.get("priceTargetHigh") or item.get("targetHigh") or 0.0),
                float(item.get("priceTargetLow") or item.get("targetLow") or 0.0),
                float(item.get("priceTargetAverage") or item.get("targetMean") or 0.0),
                float(item.get("priceTargetCurrent") or item.get("currentPrice") or 0.0),
                int(item.get("numberOfAnalysts") or item.get("analystRatings") or 0),
                item,
            )
        )
    if not rows:
        return
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        await conn.execute(DDL)
        await conn.executemany(
            """
            INSERT INTO fmp_price_targets_v4(
                symbol,
                published_date,
                target_high,
                target_low,
                target_avg,
                target_current,
                analyst_count,
                meta
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
            ON CONFLICT(symbol, published_date)
            DO UPDATE SET
              target_high = EXCLUDED.target_high,
              target_low = EXCLUDED.target_low,
              target_avg = EXCLUDED.target_avg,
              target_current = EXCLUDED.target_current,
              analyst_count = EXCLUDED.analyst_count,
              meta = EXCLUDED.meta,
              updated_at = NOW()
            """,
            rows,
        )
    finally:
        await conn.close()

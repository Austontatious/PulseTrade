import datetime as dt
import os
from typing import Any, Dict, List

import asyncpg
import httpx

from ..config import DB_DSN

API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v4/historical/social-sentiment"
LOOKBACK_DAYS = int(os.getenv("FMP_SENTIMENT_LOOKBACK_DAYS", "30"))
LIMIT = int(os.getenv("FMP_SENTIMENT_LIMIT", "100"))

DDL = """
CREATE TABLE IF NOT EXISTS fmp_social_sentiment (
  symbol TEXT NOT NULL,
  as_of DATE NOT NULL,
  source TEXT NOT NULL,
  item_id TEXT NOT NULL,
  sentiment_score DOUBLE PRECISION,
  positive_count INTEGER,
  negative_count INTEGER,
  neutral_count INTEGER,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(symbol, as_of, source, item_id)
);
"""


def _as_date(value: Any) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except Exception:
        return None


async def fetch_sentiment(symbol: str) -> None:
    if not API_KEY:
        return
    params: Dict[str, Any] = {"symbol": symbol, "apikey": API_KEY, "limit": LIMIT}
    if LOOKBACK_DAYS > 0:
        start = dt.date.today() - dt.timedelta(days=LOOKBACK_DAYS)
        params["from"] = start.isoformat()
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
        as_of = _as_date(item.get("date") or item.get("publicationDate") or item.get("publishedDate"))
        if not as_of:
            continue
        src = str(item.get("source") or "fmp").strip() or "fmp"
        item_id = str(
            item.get("articleId")
            or item.get("id")
            or item.get("url")
            or item.get("title")
            or f"{symbol}_{as_of.isoformat()}"
        )
        rows.append(
            (
                symbol,
                as_of,
                src,
                item_id,
                float(item.get("sentiment", 0.0) or 0.0),
                int(item.get("positiveMention", item.get("positiveScore", 0)) or 0),
                int(item.get("negativeMention", item.get("negativeScore", 0)) or 0),
                int(item.get("neutralMention", item.get("neutralScore", 0)) or 0),
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
            INSERT INTO fmp_social_sentiment(
                symbol,
                as_of,
                source,
                item_id,
                sentiment_score,
                positive_count,
                negative_count,
                neutral_count,
                meta
            )
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
            ON CONFLICT(symbol, as_of, source, item_id)
            DO UPDATE SET
              sentiment_score = EXCLUDED.sentiment_score,
              positive_count = EXCLUDED.positive_count,
              negative_count = EXCLUDED.negative_count,
              neutral_count = EXCLUDED.neutral_count,
              updated_at = NOW(),
              meta = EXCLUDED.meta
            """,
            rows,
        )
    finally:
        await conn.close()

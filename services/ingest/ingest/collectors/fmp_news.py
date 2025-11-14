import os
import datetime as dt
from typing import Iterable, List
import asyncpg
import httpx
from ..config import DB_DSN

API_KEY = os.getenv("FMP_API_KEY")
BASE = "https://financialmodelingprep.com/api/v3/stock_news"

DDL = """
CREATE TABLE IF NOT EXISTS fmp_news (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  title TEXT,
  site TEXT,
  url TEXT,
  text TEXT,
  meta JSONB DEFAULT '{}'::jsonb
);
"""

HEADLINE_RISK_COUNT = int(os.getenv("HEADLINE_RISK_COUNT", "5"))
HEADLINE_RISK_WINDOW_MIN = int(os.getenv("HEADLINE_RISK_WINDOW_MIN", "60"))
HEADLINE_RISK_TTL_MIN = int(os.getenv("HEADLINE_RISK_TTL_MIN", "60"))

def _chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

async def fetch_news_for(symbols: List[str]) -> None:
    if not API_KEY or not symbols:
        return
    async with httpx.AsyncClient(timeout=20) as client:
        conn = await asyncpg.connect(dsn=DB_DSN)
        try:
            await conn.execute(DDL)
            for batch in _chunks(symbols, 10):
                params = {"tickers": ",".join(batch), "limit": 20, "apikey": API_KEY}
                r = await client.get(BASE, params=params)
                if r.status_code != 200:
                    continue
                data = r.json()
                rows = []
                now = dt.datetime.now(dt.timezone.utc)
                for item in data:
                    sym = (item.get("symbol") or "").strip().upper()
                    if not sym:
                        continue
                    ts_raw = item.get("publishedDate")
                    try:
                        ts = dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00")) if ts_raw else now
                    except Exception:
                        ts = now
                    rows.append((ts, sym, item.get("title"), item.get("site"), item.get("url"), item.get("text"), item))
                if rows:
                    await conn.executemany(
                        """
                        INSERT INTO fmp_news(ts, symbol, title, site, url, text, meta)
                        VALUES($1,$2,$3,$4,$5,$6,$7)
                        ON CONFLICT DO NOTHING
                        """,
                        rows,
                    )
            # Headline risk: trip symbol breaker if headlines spike
            window_secs = HEADLINE_RISK_WINDOW_MIN * 60
            since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=window_secs)
            counts = await conn.fetch(
                """
                SELECT symbol, COUNT(*) AS n
                FROM fmp_news
                WHERE ts >= $1 AND symbol = ANY($2)
                GROUP BY symbol
                HAVING COUNT(*) >= $3
                """,
                since,
                symbols,
                HEADLINE_RISK_COUNT,
            )
            for row in counts:
                sym = row["symbol"]
                exists = await conn.fetchrow(
                    """
                    SELECT 1 FROM circuit_breakers
                    WHERE scope='symbol' AND key=$1 AND active=TRUE
                      AND (expires_at IS NULL OR expires_at > NOW())
                    LIMIT 1
                    """,
                    sym,
                )
                if not exists:
                    await conn.execute(
                        """
                        INSERT INTO circuit_breakers(ts, scope, key, active, reason, expires_at, meta)
                        VALUES(NOW(), 'symbol', $1, TRUE, 'headline_risk', NOW() + make_interval(mins => $2::int), '{}')
                        """,
                        sym,
                        HEADLINE_RISK_TTL_MIN,
                    )
        finally:
            await conn.close()


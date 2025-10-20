import os
import asyncio
from typing import List, Set
import asyncpg
from .config import DB_DSN

DISCOVERY_WINDOW_MIN = int(os.getenv("SYMBOL_DISCOVERY_WINDOW_MIN", "30"))
DISCOVERY_MIN_MSG_RATE = float(os.getenv("SYMBOL_DISCOVERY_MIN_MSG_RATE", "0.2"))
DISCOVERY_MIN_SENTI_ABS = float(os.getenv("SYMBOL_DISCOVERY_MIN_SENTI_ABS", "0.1"))
DISCOVERY_MAX_ADD = int(os.getenv("SYMBOL_DISCOVERY_MAX_ADD", "10"))
DISCOVERY_SLEEP_SECS = int(os.getenv("SYMBOL_DISCOVERY_SECS", "60"))

async def _candidate_tickers(conn: asyncpg.Connection) -> List[str]:
    rows = await conn.fetch(
        """
        SELECT ticker,
               max(msg_rate) AS rate,
               avg(senti_mean) AS senti
        FROM social_features
        WHERE ts > NOW() - make_interval(mins => $1::int)
          AND ticker IS NOT NULL
          AND ticker NOT LIKE '%USD'
        GROUP BY ticker
        ORDER BY rate DESC
        LIMIT 100
        """,
        DISCOVERY_WINDOW_MIN,
    )
    out = []
    for r in rows:
        rate = float(r["rate"]) if r["rate"] is not None else 0.0
        senti = float(r["senti"]) if r["senti"] is not None else 0.0
        if rate >= DISCOVERY_MIN_MSG_RATE and abs(senti) >= DISCOVERY_MIN_SENTI_ABS:
            out.append(r["ticker"])
    return out[:DISCOVERY_MAX_ADD]

async def _existing_equities(conn: asyncpg.Connection) -> Set[str]:
    rows = await conn.fetch("SELECT ticker FROM symbols WHERE class='equity'")
    return {r["ticker"] for r in rows}

async def run_symbol_discovery() -> None:
    while True:
        try:
            conn = await asyncpg.connect(dsn=DB_DSN)
            try:
                candidates = await _candidate_tickers(conn)
                existing = await _existing_equities(conn)
                to_add = [t for t in candidates if t not in existing]
                if to_add:
                    rows = [(t, 'equity', 'ALPACA') for t in to_add]
                    await conn.executemany(
                        "INSERT INTO symbols(ticker, class, venue) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
                        rows,
                    )
                    print("symbol discovery added:", to_add)
            finally:
                await conn.close()
        except Exception as e:  # pragma: no cover
            print("symbol discovery error:", e)
        await asyncio.sleep(DISCOVERY_SLEEP_SECS)

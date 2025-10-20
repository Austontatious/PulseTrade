import asyncio
import os
from typing import List
import asyncpg
import httpx
from bs4 import BeautifulSoup
import re
from .config import DB_DSN

WIKI_SNP500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKI_NDX100 = "https://en.wikipedia.org/wiki/Nasdaq-100"

async def _insert_symbols(conn: asyncpg.Connection, tickers: List[str], venue: str):
    rows = [(t, 'equity', venue) for t in tickers]
    await conn.executemany(
        "INSERT INTO symbols(ticker, class, venue) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
        rows,
    )

_SYM_RE = re.compile(r"^[A-Z][A-Z0-9\.-]{0,6}$")

def _clean_ticker(t: str) -> str:
    s = t.strip().upper().replace(".", "-")
    s = s.replace("\u200b", "")  # zero-width spaces if present
    return s

def _valid_ticker(s: str) -> bool:
    if not s or " " in s:
        return False
    return bool(_SYM_RE.match(s))

async def seed_sp500(conn: asyncpg.Connection):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(WIKI_SNP500)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'}) or soup.find('table')
        tickers: List[str] = []
        if table:
            for row in table.find_all('tr')[1:]:
                cols = row.find_all('td')
                if not cols:
                    continue
                sym = cols[0].get_text()
                if sym:
                    symc = _clean_ticker(sym)
                    if _valid_ticker(symc):
                        tickers.append(symc)
        if tickers:
            await _insert_symbols(conn, tickers, 'ALPACA')
            print(f"seeded S&P500: {len(tickers)} symbols")

async def seed_ndx100(conn: asyncpg.Connection):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(WIKI_NDX100)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        tickers: List[str] = []
        # Find table that contains a header cell with text 'Ticker' or 'Symbol'
        tables = soup.find_all('table')
        target_tables = []
        for tbl in tables:
            hdrs = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
            if any(h in ("ticker", "symbol") for h in hdrs):
                target_tables.append(tbl)
        for tbl in (target_tables or tables):
            rows = tbl.find_all('tr')[1:]
            for row in rows:
                cols = row.find_all('td')
                if not cols:
                    continue
                # Heuristic: prefer first cell that looks like a ticker
                candidates = [c.get_text() for c in cols[:2]]
                for sym in candidates:
                    symc = _clean_ticker(sym)
                    if _valid_ticker(symc):
                        tickers.append(symc)
                        break
        # Deduplicate
        tickers = list(dict.fromkeys(tickers))
        if tickers:
            await _insert_symbols(conn, tickers, 'ALPACA')
            print(f"seeded NASDAQ100: {len(tickers)} symbols")

async def run_universe_seeders():
    if os.getenv("ENABLE_UNIVERSE_SEED", "1") != "1":
        return
    try:
        conn = await asyncpg.connect(dsn=DB_DSN)
        try:
            await seed_sp500(conn)
            await seed_ndx100(conn)
            # Cleanup any invalid symbols from prior runs
            await conn.execute(
                "DELETE FROM symbols WHERE class='equity' AND NOT (ticker ~ '^[A-Z][A-Z0-9\.-]{0,6}$')"
            )
        finally:
            await conn.close()
    except Exception as e:  # pragma: no cover
        print("universe seeding error:", e)

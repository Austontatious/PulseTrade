import os
import asyncpg
import httpx
from bs4 import BeautifulSoup
from ..config import DB_DSN

BASE_API = "https://api.stocktwits.com/api/2/trending/symbols.json"
BASE_WEB = "https://stocktwits.com/"

def _clean(sym: str) -> str:
    return sym.strip().upper().replace('.', '-')

async def _insert_symbols(conn: asyncpg.Connection, symbols: list[str]):
    if not symbols:
        return
    rows = [(s, 'equity', 'ALPACA') for s in symbols]
    await conn.executemany(
        "INSERT INTO symbols(ticker, class, venue) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
        rows,
    )

async def _fetch_api(client: httpx.AsyncClient, token: str | None) -> list[str]:
    params = {}
    if token:
        params["access_token"] = token
    r = await client.get(BASE_API, params=params, timeout=10)
    if r.status_code != 200:
        return []
    data = r.json()
    out: list[str] = []
    for item in data.get("symbols", []):
        s = item.get("symbol") or ""
        s = _clean(s)
        if s and ' ' not in s:
            out.append(s)
    # dedupe
    return list(dict.fromkeys(out))

async def _fetch_scrape(client: httpx.AsyncClient) -> list[str]:
    r = await client.get(BASE_WEB, timeout=10)
    if r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, 'html.parser')
    out: list[str] = []
    # find cashtag anchors like $AAPL
    for a in soup.find_all('a'):
        txt = (a.get_text() or '').strip()
        if len(txt) >= 2 and txt.startswith('$'):
            sym = _clean(txt[1:])
            if sym and sym.isalnum():
                out.append(sym)
    return list(dict.fromkeys(out))

async def fetch_trending_symbols() -> None:
    token = os.getenv("STOCKTWITS_TOKEN")
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        async with httpx.AsyncClient() as client:
            syms = await _fetch_api(client, token)
            if not syms:
                syms = await _fetch_scrape(client)
            if syms:
                await _insert_symbols(conn, syms)
                print(f"stocktwits trending added: {len(syms)} symbols")
    finally:
        await conn.close()


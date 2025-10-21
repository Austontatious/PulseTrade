import os
import datetime as dt
import asyncpg
import httpx
from ..config import DB_DSN

API_KEY = os.getenv("FMP_API_KEY")

async def fetch_earnings_calendar() -> None:
    if not API_KEY:
        return
    today = dt.date.today()
    # Pull today +/- 1 day
    start = today - dt.timedelta(days=1)
    end = today + dt.timedelta(days=1)
    url = "https://financialmodelingprep.com/api/v3/earning_calendar"
    params = {"from": start.isoformat(), "to": end.isoformat(), "apikey": API_KEY}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return
        data = r.json()
    # Insert blackout windows around reported time
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        rows = []
        blackout_min = int(os.getenv("EARNINGS_BLACKOUT_MIN", "30"))
        for item in data:
            sym = (item.get("symbol") or "").strip().upper()
            date_str = item.get("date") or item.get("reportDate")
            when = None
            try:
                when = dt.datetime.fromisoformat(date_str)
            except Exception:
                # Fallback: use date at 00:00Z
                try:
                    when = dt.datetime.fromisoformat(date_str + "T00:00:00")
                except Exception:
                    continue
            when = when.replace(tzinfo=dt.timezone.utc)
            start_ts = when - dt.timedelta(minutes=blackout_min)
            end_ts = when + dt.timedelta(minutes=blackout_min)
            rows.append((sym, start_ts, end_ts, "earnings", {}))
        if rows:
            await conn.executemany(
                """
                INSERT INTO event_windows(ticker, start_ts, end_ts, kind, meta)
                VALUES($1,$2,$3,$4,$5)
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
            print(f"earnings calendar windows upserted: {len(rows)}")
    finally:
        await conn.close()


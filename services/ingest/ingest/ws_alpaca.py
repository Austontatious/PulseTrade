import os
import json
import asyncio
import datetime as dt
import websockets
import asyncpg
from .config import DB_DSN

WS_URL = os.getenv("ALPACA_DATA_WS_URL", "wss://stream.data.alpaca.markets/v2/sip")
API_KEY = os.getenv("ALPACA_API_KEY_ID")
API_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
SYMBOLS = [s.strip() for s in os.getenv("ALPACA_SYMBOLS", "AAPL,MSFT,SPY").split(",") if s.strip()]

async def run_alpaca() -> None:
    if not API_KEY or not API_SECRET:
        print("alpaca disabled (missing keys)")
        return
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                await ws.send(json.dumps({"action": "auth", "key": API_KEY, "secret": API_SECRET}))
                await ws.send(json.dumps({"action": "subscribe", "trades": SYMBOLS}))
                conn = await asyncpg.connect(dsn=DB_DSN)
                try:
                    while True:
                        message = await ws.recv()
                        payload = json.loads(message)
                        if isinstance(payload, list):
                            for event in payload:
                                if event.get("T") == "t":
                                    ts = dt.datetime.fromtimestamp(event["t"] / 1e9, tz=dt.timezone.utc)
                                    price = float(event.get("p", 0.0))
                                    size = float(event.get("s") or 0.0)
                                    await conn.execute(
                                        "INSERT INTO trades(ts,ticker,price,size,venue,meta) VALUES($1,$2,$3,$4,$5,$6)",
                                        ts,
                                        event.get("S"),
                                        price,
                                        size,
                                        "ALPACA",
                                        event,
                                    )
                finally:
                    await conn.close()
        except Exception as exc:  # pragma: no cover - network loop guard
            print("alpaca ws error:", exc)
            await asyncio.sleep(3)

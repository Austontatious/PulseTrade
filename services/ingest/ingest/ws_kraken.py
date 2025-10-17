import asyncio, json, asyncpg, websockets, datetime as dt
from .config import DB_DSN

PAIR_MAP = {"XBT/USD":"BTCUSD","ETH/USD":"ETHUSD"}

async def run_kraken():
    uri = "wss://ws.kraken.com"
    subscribe = {"event":"subscribe","pair": list(PAIR_MAP.keys()), "subscription":{"name":"ticker"}}
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                await ws.send(json.dumps(subscribe))
                conn = await asyncpg.connect(dsn=DB_DSN)
                try:
                    while True:
                        raw = await ws.recv()
                        d = json.loads(raw)
                        if isinstance(d, list) and len(d) > 1 and isinstance(d[1], dict):
                            info = d[1]; pair = d[3]
                            ticker = PAIR_MAP.get(pair, pair.replace("/",""))
                            price = float(info.get("c",[0,0])[0])
                            ts = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
                            await conn.execute(
                                "INSERT INTO trades(ts,ticker,price,venue,meta) VALUES($1,$2,$3,$4,$5)",
                                ts, ticker, price, "KRAKEN", json.dumps({"raw":d}))
                finally:
                    await conn.close()
        except Exception as e:
            print("kraken ws error:", e)
            await asyncio.sleep(3)

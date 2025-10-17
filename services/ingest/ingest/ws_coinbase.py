import asyncio, json, asyncpg, websockets, datetime as dt
from .config import DB_DSN

PRODUCTS = ["BTC-USD", "ETH-USD"]  # extend as needed

async def run_coinbase():
    uri = "wss://ws-feed.exchange.coinbase.com"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                sub = {"type":"subscribe","channels":[{"name":"ticker","product_ids":PRODUCTS}]}
                await ws.send(json.dumps(sub))
                conn = await asyncpg.connect(dsn=DB_DSN)
                try:
                    while True:
                        msg = await ws.recv()
                        d = json.loads(msg)
                        if d.get("type") == "ticker":
                            ts = dt.datetime.fromisoformat(d["time"].replace("Z","+00:00"))
                            ticker = d["product_id"].replace("-","")
                            price = float(d["price"])
                            size = float(d.get("last_size") or 0.0)
                            await conn.execute(
                              "INSERT INTO trades(ts,ticker,price,size,venue,meta) VALUES($1,$2,$3,$4,$5,$6)",
                              ts, ticker, price, size, "COINBASE", json.dumps({"raw":d}))
                finally:
                    await conn.close()
        except Exception as e:
            print("coinbase ws error:", e)
            await asyncio.sleep(3)

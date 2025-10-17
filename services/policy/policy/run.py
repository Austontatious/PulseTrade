import asyncio
import asyncpg
import json
import os
try:
    from .executor import maybe_submit_order
except ImportError:  # pragma: no cover
    async def maybe_submit_order(*args, **kwargs):  # type: ignore
        return None

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

THRESH = 0.001  # trade when forecast deviates >0.1% from last price (toy)

async def step(conn):
    q = """WITH latest AS (
             SELECT DISTINCT ON (ticker) ts,ticker,mean,lower,upper
             FROM forecasts WHERE horizon='1m' ORDER BY ticker, ts DESC
           ),
           px AS (
             SELECT DISTINCT ON (ticker) ts,ticker,price FROM trades ORDER BY ticker, ts DESC
           )
           SELECT l.ticker, l.mean, p.price
             FROM latest l JOIN px p USING (ticker);"""
    rows = await conn.fetch(q)
    for r in rows:
        ticker = r["ticker"]; mean = float(r["mean"]); price = float(r["price"])
        dev = (mean/price) - 1.0
        qty = 0.0; side = None
        if dev > THRESH:
            side = "buy"; qty = 0.001  # toy units
        elif dev < -THRESH:
            side = "sell"; qty = 0.001
        if side:
            meta = {
                "source": "policy",
                "signal_dev": dev,
            }
            ack = await maybe_submit_order(ticker, side, qty, price)
            if ack:
                meta["alpaca"] = {
                    "id": ack.get("id"),
                    "status": ack.get("status"),
                    "submitted_at": ack.get("submitted_at"),
                }
            await conn.execute(
                """INSERT INTO fills(ts,ticker,side,qty,price,venue,meta)
                   VALUES (NOW(), $1,$2,$3,$4,$5,$6)""",
                ticker,
                side,
                qty,
                price,
                "SIM",
                json.dumps(meta),
            )

async def main():
    while True:
        conn = await asyncpg.connect(dsn=DB_DSN)
        try:
            await step(conn)
        finally:
            await conn.close()
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())

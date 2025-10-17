from .app import app
import asyncpg, os, numpy as np, pandas as pd, asyncio

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

@app.task
def build_minute_features(ticker: str):
    """Example feature: last-60s return & volatility for ticker"""
    async def _run():
        conn = await asyncpg.connect(dsn=DB_DSN)
        try:
            q = """SELECT ts, price FROM trades WHERE ticker=$1 AND ts > NOW() - INTERVAL '2 minutes' ORDER BY ts ASC"""
            rows = await conn.fetch(q, ticker)
            if len(rows) < 2:
                return {"ticker":ticker, "ok":False}
            prices = pd.Series([r["price"] for r in rows], index=[r["ts"] for r in rows])
            ret_60s = (prices.iloc[-1] / prices.iloc[0]) - 1.0
            vol = prices.pct_change().std() * np.sqrt(60)
            # You can persist features table here (omitted for brevity)
            return {"ticker":ticker, "ok":True, "ret_60s":float(ret_60s), "vol":float(vol)}
        finally:
            await conn.close()
    return asyncio.run(_run())
